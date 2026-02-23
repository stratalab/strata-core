//! Minimal cross-platform dynamic library loading (no external crates).
//!
//! Provides [`DynLib`] for loading shared libraries and resolving symbols at
//! runtime. Used by the llama.cpp FFI layer to load `libllama` without any
//! build-time dependency.

use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

/// Handle to a dynamically loaded shared library.
pub struct DynLib {
    handle: *mut c_void,
}

impl std::fmt::Debug for DynLib {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynLib")
            .field("handle", &self.handle)
            .finish()
    }
}

// SAFETY: The library handle is a process-global resource; sharing across
// threads is safe as long as callers ensure symbol usage is thread-safe.
unsafe impl Send for DynLib {}
unsafe impl Sync for DynLib {}

impl DynLib {
    /// Open a shared library by name or path.
    ///
    /// On Unix, wraps `dlopen` with `RTLD_NOW | RTLD_LOCAL`.
    /// On Windows, wraps `LoadLibraryA`.
    pub fn open(name: &CStr) -> Result<Self, String> {
        #[cfg(unix)]
        {
            // SAFETY: name is a valid C string. dlopen with RTLD_NOW resolves
            // all symbols immediately; RTLD_LOCAL keeps them in this handle.
            let handle = unsafe { dlopen(name.as_ptr(), RTLD_NOW | RTLD_LOCAL) };
            if handle.is_null() {
                let err = unsafe { dlerror() };
                let msg = if err.is_null() {
                    "unknown dlopen error".to_string()
                } else {
                    unsafe { CStr::from_ptr(err) }
                        .to_string_lossy()
                        .into_owned()
                };
                return Err(msg);
            }
            Ok(Self { handle })
        }

        #[cfg(windows)]
        {
            let handle = unsafe { LoadLibraryA(name.as_ptr()) };
            if handle.is_null() {
                return Err(format!("LoadLibraryA failed for {:?}", name));
            }
            Ok(Self {
                handle: handle as *mut c_void,
            })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = name;
            Err("dynamic library loading not supported on this platform".to_string())
        }
    }

    /// Look up a symbol by name, returning a raw pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure the returned pointer is cast to the correct
    /// function signature before use.
    pub unsafe fn sym(&self, name: &CStr) -> Result<*mut c_void, String> {
        #[cfg(unix)]
        {
            // Clear any previous error.
            dlerror();
            let ptr = dlsym(self.handle, name.as_ptr());
            let err = dlerror();
            if !err.is_null() {
                let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
                return Err(msg);
            }
            Ok(ptr)
        }

        #[cfg(windows)]
        {
            let ptr = GetProcAddress(self.handle as _, name.as_ptr());
            if ptr.is_null() {
                return Err(format!("GetProcAddress failed for {:?}", name));
            }
            Ok(ptr as *mut c_void)
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = name;
            Err("dynamic library loading not supported on this platform".to_string())
        }
    }
}

impl Drop for DynLib {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            #[cfg(unix)]
            unsafe {
                dlclose(self.handle);
            }

            #[cfg(windows)]
            unsafe {
                FreeLibrary(self.handle as _);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Platform-aware library name helpers
// ---------------------------------------------------------------------------

/// Returns the platform-specific shared library file extension.
pub fn lib_extension() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "so"
    }
    #[cfg(target_os = "macos")]
    {
        "dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "dll"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        "so"
    }
}

/// Returns the platform-specific libllama filename (e.g. `libllama.so`).
pub fn libllama_filename() -> String {
    #[cfg(target_os = "windows")]
    {
        "llama.dll".to_string()
    }
    #[cfg(not(target_os = "windows"))]
    {
        format!("libllama.{}", lib_extension())
    }
}

// ---------------------------------------------------------------------------
// Unix (Linux + macOS) bindings
// ---------------------------------------------------------------------------

#[cfg(unix)]
const RTLD_NOW: i32 = 2;
#[cfg(unix)]
const RTLD_LOCAL: i32 = 0;

#[cfg(unix)]
extern "C" {
    fn dlopen(filename: *const c_char, flags: i32) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> i32;
    fn dlerror() -> *const c_char;
}

// ---------------------------------------------------------------------------
// Windows bindings
// ---------------------------------------------------------------------------

#[cfg(windows)]
extern "system" {
    fn LoadLibraryA(name: *const c_char) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const c_char) -> *mut c_void;
    fn FreeLibrary(module: *mut c_void) -> i32;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_nonexistent_library_returns_error() {
        let result = DynLib::open(c"nonexistent_library_that_does_not_exist_12345");
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Error message should be descriptive, not empty
        assert!(!err.is_empty(), "error message should not be empty");
        // On Unix, dlopen errors typically mention the library name
        #[cfg(unix)]
        assert!(
            err.contains("nonexistent_library"),
            "error should mention the library name: {err}"
        );
    }

    #[test]
    fn sym_on_invalid_symbol_returns_error_with_symbol_name() {
        // Load a library we know exists (libc on Unix)
        #[cfg(target_os = "linux")]
        let lib = DynLib::open(c"libc.so.6");
        #[cfg(target_os = "macos")]
        let lib = DynLib::open(c"libSystem.B.dylib");
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return; // Skip on unsupported platforms

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let lib = lib.expect("should be able to load system libc");
            let result = unsafe { lib.sym(c"__this_symbol_definitely_does_not_exist_xyz__") };
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(!err.is_empty(), "error message should not be empty");
            // dlsym errors typically mention the symbol name
            assert!(
                err.contains("__this_symbol_definitely_does_not_exist_xyz__"),
                "error should mention the symbol name: {err}"
            );
        }
    }

    #[test]
    fn sym_on_valid_symbol_succeeds() {
        #[cfg(target_os = "linux")]
        let lib = DynLib::open(c"libc.so.6");
        #[cfg(target_os = "macos")]
        let lib = DynLib::open(c"libSystem.B.dylib");
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return;

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let lib = lib.expect("should be able to load system libc");
            // `strlen` exists in every libc
            let result = unsafe { lib.sym(c"strlen") };
            assert!(result.is_ok(), "strlen should resolve: {:?}", result);
            assert!(!result.unwrap().is_null(), "strlen pointer should be non-null");
        }
    }

    #[test]
    fn drop_does_not_panic() {
        // Open and immediately drop
        #[cfg(target_os = "linux")]
        {
            let lib = DynLib::open(c"libc.so.6").unwrap();
            drop(lib);
        }
        #[cfg(target_os = "macos")]
        {
            let lib = DynLib::open(c"libSystem.B.dylib").unwrap();
            drop(lib);
        }
        // If we get here without panic, the test passes
    }

    #[test]
    fn drop_null_handle_does_not_panic() {
        // Simulate a null handle (shouldn't happen normally, but test the guard)
        let lib = DynLib {
            handle: std::ptr::null_mut(),
        };
        drop(lib);
    }

    #[test]
    fn lib_extension_is_correct() {
        let ext = lib_extension();
        #[cfg(target_os = "linux")]
        assert_eq!(ext, "so");
        #[cfg(target_os = "macos")]
        assert_eq!(ext, "dylib");
        #[cfg(target_os = "windows")]
        assert_eq!(ext, "dll");
        // Always non-empty
        assert!(!ext.is_empty());
    }

    #[test]
    fn libllama_filename_is_correct() {
        let name = libllama_filename();
        #[cfg(target_os = "linux")]
        assert_eq!(name, "libllama.so");
        #[cfg(target_os = "macos")]
        assert_eq!(name, "libllama.dylib");
        #[cfg(target_os = "windows")]
        assert_eq!(name, "llama.dll");
        assert!(!name.is_empty());
    }

    #[test]
    fn multiple_open_close_cycles_no_leak() {
        // Open and close multiple times — should not leak or crash
        for _ in 0..5 {
            #[cfg(target_os = "linux")]
            {
                let lib = DynLib::open(c"libc.so.6").unwrap();
                drop(lib);
            }
            #[cfg(target_os = "macos")]
            {
                let lib = DynLib::open(c"libSystem.B.dylib").unwrap();
                drop(lib);
            }
        }
    }

    #[test]
    fn debug_impl_shows_struct_name_and_handle() {
        let lib = DynLib {
            handle: std::ptr::null_mut(),
        };
        let dbg = format!("{:?}", lib);
        assert!(dbg.contains("DynLib"), "Debug output should contain struct name: {dbg}");
        assert!(dbg.contains("handle"), "Debug output should contain field name: {dbg}");
        // Don't drop this normally — null handle is safe due to our guard
    }

    #[test]
    fn resolved_symbol_is_callable() {
        // Verify that a resolved function pointer can actually be called
        #[cfg(target_os = "linux")]
        let lib = DynLib::open(c"libc.so.6");
        #[cfg(target_os = "macos")]
        let lib = DynLib::open(c"libSystem.B.dylib");
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return;

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let lib = lib.expect("should load system libc");
            let ptr = unsafe { lib.sym(c"strlen") }.expect("strlen should resolve");

            // Cast to the correct function signature and call it
            let strlen_fn: unsafe extern "C" fn(*const c_char) -> usize =
                unsafe { std::mem::transmute(ptr) };
            let test_str = c"hello";
            let len = unsafe { strlen_fn(test_str.as_ptr()) };
            assert_eq!(len, 5, "strlen(\"hello\") should be 5");
        }
    }

    #[test]
    fn open_same_library_twice_succeeds() {
        // Opening the same library twice should work (refcount in dlopen)
        #[cfg(target_os = "linux")]
        {
            let lib1 = DynLib::open(c"libc.so.6").unwrap();
            let lib2 = DynLib::open(c"libc.so.6").unwrap();
            // Both handles should be valid (non-null)
            let dbg1 = format!("{:?}", lib1);
            let dbg2 = format!("{:?}", lib2);
            assert!(dbg1.contains("DynLib"));
            assert!(dbg2.contains("DynLib"));
            // Dropping both should not crash
        }
        #[cfg(target_os = "macos")]
        {
            let lib1 = DynLib::open(c"libSystem.B.dylib").unwrap();
            let lib2 = DynLib::open(c"libSystem.B.dylib").unwrap();
            let dbg1 = format!("{:?}", lib1);
            let dbg2 = format!("{:?}", lib2);
            assert!(dbg1.contains("DynLib"));
            assert!(dbg2.contains("DynLib"));
        }
    }
}
