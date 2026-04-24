//! YCSB workload definitions and key distribution generators.
//!
//! Implements standard YCSB workloads A-F with Zipfian, Uniform, and Latest
//! key distribution generators following the original YCSB specification.

// ---------------------------------------------------------------------------
// Operation types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operation {
    Read,
    Update,
    Insert,
    Scan,
    ReadModifyWrite,
}

// ---------------------------------------------------------------------------
// Workload specification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WorkloadSpec {
    pub name: &'static str,
    pub label: char,
    pub description: &'static str,
    pub read_proportion: f64,
    pub update_proportion: f64,
    pub insert_proportion: f64,
    pub scan_proportion: f64,
    pub rmw_proportion: f64,
    pub distribution: Distribution,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum Distribution {
    Zipfian,
    Uniform,
    Latest,
}

impl Distribution {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Zipfian => "zipfian",
            Self::Uniform => "uniform",
            Self::Latest => "latest",
        }
    }
}

impl WorkloadSpec {
    /// Select an operation based on weighted proportions using a random value in [0, 1).
    pub fn choose_operation(&self, r: f64) -> Operation {
        let mut cumulative = self.read_proportion;
        if r < cumulative {
            return Operation::Read;
        }
        cumulative += self.update_proportion;
        if r < cumulative {
            return Operation::Update;
        }
        cumulative += self.insert_proportion;
        if r < cumulative {
            return Operation::Insert;
        }
        cumulative += self.scan_proportion;
        if r < cumulative {
            return Operation::Scan;
        }
        Operation::ReadModifyWrite
    }

    /// Short summary like "50r/50u, Zipfian"
    pub fn mix_label(&self) -> String {
        let mut parts = Vec::new();
        if self.read_proportion > 0.0 {
            parts.push(format!("{}r", (self.read_proportion * 100.0) as u32));
        }
        if self.update_proportion > 0.0 {
            parts.push(format!("{}u", (self.update_proportion * 100.0) as u32));
        }
        if self.insert_proportion > 0.0 {
            parts.push(format!("{}i", (self.insert_proportion * 100.0) as u32));
        }
        if self.scan_proportion > 0.0 {
            parts.push(format!("{}s", (self.scan_proportion * 100.0) as u32));
        }
        if self.rmw_proportion > 0.0 {
            parts.push(format!("{}rmw", (self.rmw_proportion * 100.0) as u32));
        }
        format!("{}, {}", parts.join("/"), self.distribution.label())
    }
}

// ---------------------------------------------------------------------------
// Standard YCSB workloads
// ---------------------------------------------------------------------------

pub const WORKLOAD_A: WorkloadSpec = WorkloadSpec {
    name: "Update Heavy",
    label: 'a',
    description: "50% read, 50% update — session store",
    read_proportion: 0.50,
    update_proportion: 0.50,
    insert_proportion: 0.0,
    scan_proportion: 0.0,
    rmw_proportion: 0.0,
    distribution: Distribution::Zipfian,
};

pub const WORKLOAD_B: WorkloadSpec = WorkloadSpec {
    name: "Read Mostly",
    label: 'b',
    description: "95% read, 5% update — photo tagging",
    read_proportion: 0.95,
    update_proportion: 0.05,
    insert_proportion: 0.0,
    scan_proportion: 0.0,
    rmw_proportion: 0.0,
    distribution: Distribution::Zipfian,
};

pub const WORKLOAD_C: WorkloadSpec = WorkloadSpec {
    name: "Read Only",
    label: 'c',
    description: "100% read — user profile cache",
    read_proportion: 1.0,
    update_proportion: 0.0,
    insert_proportion: 0.0,
    scan_proportion: 0.0,
    rmw_proportion: 0.0,
    distribution: Distribution::Zipfian,
};

pub const WORKLOAD_D: WorkloadSpec = WorkloadSpec {
    name: "Read Latest",
    label: 'd',
    description: "95% read, 5% insert — user status",
    read_proportion: 0.95,
    update_proportion: 0.0,
    insert_proportion: 0.05,
    scan_proportion: 0.0,
    rmw_proportion: 0.0,
    distribution: Distribution::Latest,
};

pub const WORKLOAD_E: WorkloadSpec = WorkloadSpec {
    name: "Short Ranges",
    label: 'e',
    description: "5% insert, 95% scan — threaded conversations",
    read_proportion: 0.0,
    update_proportion: 0.0,
    insert_proportion: 0.05,
    scan_proportion: 0.95,
    rmw_proportion: 0.0,
    distribution: Distribution::Zipfian,
};

pub const WORKLOAD_F: WorkloadSpec = WorkloadSpec {
    name: "Read-Modify-Write",
    label: 'f',
    description: "50% read, 50% read-modify-write — user database",
    read_proportion: 0.50,
    update_proportion: 0.0,
    insert_proportion: 0.0,
    scan_proportion: 0.0,
    rmw_proportion: 0.50,
    distribution: Distribution::Zipfian,
};

pub const ALL_WORKLOADS: &[&WorkloadSpec] = &[
    &WORKLOAD_A,
    &WORKLOAD_B,
    &WORKLOAD_C,
    &WORKLOAD_D,
    &WORKLOAD_E,
    &WORKLOAD_F,
];

pub fn workload_by_label(label: char) -> Option<&'static WorkloadSpec> {
    ALL_WORKLOADS
        .iter()
        .find(|w| w.label == label.to_ascii_lowercase())
        .copied()
}

// ---------------------------------------------------------------------------
// Fast LCG random number generator (no external crate needed)
// ---------------------------------------------------------------------------

pub struct FastRng {
    state: u64,
}

impl FastRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x5DEECE66D,
        }
    }

    /// Returns a pseudo-random u64.
    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    /// Returns a value in [0.0, 1.0).
    #[inline]
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Returns a value in [0, n).
    #[inline]
    pub fn next_usize(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// Key distribution generators
// ---------------------------------------------------------------------------

/// Scrambled Zipfian distribution with theta=0.99.
///
/// Hot keys are spread across the keyspace via FNV hash scrambling so they
/// aren't clustered at the beginning, matching the standard YCSB implementation.
pub struct ZipfianGenerator {
    num_items: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    eta: f64,
}

impl ZipfianGenerator {
    pub fn new(num_items: usize) -> Self {
        let theta = 0.99;
        let zeta_2 = zeta(2, theta);
        let zeta_n = zeta(num_items, theta);
        Self::from_precomputed(num_items, theta, zeta_n, zeta_2)
    }

    fn from_precomputed(num_items: usize, theta: f64, zeta_n: f64, zeta_2: f64) -> Self {
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / num_items as f64).powf(1.0 - theta)) / (1.0 - zeta_2 / zeta_n);

        Self {
            num_items,
            theta,
            zeta_n,
            alpha,
            eta,
        }
    }

    /// Incrementally extend the generator to cover more items without
    /// recomputing zeta from scratch. O(new_count - old_count) instead of O(new_count).
    pub fn resize(&mut self, new_count: usize) {
        if new_count <= self.num_items {
            return;
        }
        let zeta_2 = zeta(2, self.theta);
        // Incrementally add terms for the new items
        let mut zeta_n = self.zeta_n;
        for i in self.num_items..new_count {
            zeta_n += 1.0 / ((i + 1) as f64).powf(self.theta);
        }
        *self = Self::from_precomputed(new_count, self.theta, zeta_n, zeta_2);
    }

    /// Return a Zipfian-distributed index in [0, num_items), scrambled via FNV hash.
    pub fn next(&self, rng: &mut FastRng) -> usize {
        let u = rng.next_f64();
        let uz = u * self.zeta_n;

        let raw = if uz < 1.0 {
            0
        } else if uz < 1.0 + 0.5_f64.powf(self.theta) {
            1
        } else {
            let spread = self.num_items as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha);
            (spread as usize).min(self.num_items - 1)
        };

        // FNV-1a hash scrambling so hot keys aren't sequential
        fnv_hash(raw as u64) as usize % self.num_items
    }
}

/// Uniform random key selection.
pub struct UniformGenerator {
    num_items: usize,
}

impl UniformGenerator {
    pub fn new(num_items: usize) -> Self {
        Self { num_items }
    }

    pub fn next(&self, rng: &mut FastRng) -> usize {
        rng.next_usize(self.num_items)
    }
}

/// Latest distribution — biased toward the most recently inserted keys.
///
/// Uses a Zipfian distribution over distances from the latest key.
pub struct LatestGenerator {
    max_key: usize,
    zipfian: ZipfianGenerator,
}

impl LatestGenerator {
    pub fn new(record_count: usize) -> Self {
        Self {
            max_key: record_count,
            zipfian: ZipfianGenerator::new(record_count),
        }
    }

    pub fn set_max_key(&mut self, max: usize) {
        if max != self.max_key {
            self.max_key = max;
            self.zipfian.resize(max.max(1));
        }
    }

    pub fn next(&self, rng: &mut FastRng) -> usize {
        let distance = self.zipfian.next(rng);
        if distance >= self.max_key {
            0
        } else {
            self.max_key - 1 - distance
        }
    }
}

/// Key selector that dispatches to the appropriate generator.
pub enum KeyChooser {
    Zipfian(ZipfianGenerator),
    Uniform(UniformGenerator),
    Latest(LatestGenerator),
}

impl KeyChooser {
    pub fn new(dist: Distribution, num_items: usize) -> Self {
        match dist {
            Distribution::Zipfian => KeyChooser::Zipfian(ZipfianGenerator::new(num_items)),
            Distribution::Uniform => KeyChooser::Uniform(UniformGenerator::new(num_items)),
            Distribution::Latest => KeyChooser::Latest(LatestGenerator::new(num_items)),
        }
    }

    pub fn next(&self, rng: &mut FastRng) -> usize {
        match self {
            KeyChooser::Zipfian(g) => g.next(rng),
            KeyChooser::Uniform(g) => g.next(rng),
            KeyChooser::Latest(g) => g.next(rng),
        }
    }

    pub fn set_max_key(&mut self, max: usize) {
        if let KeyChooser::Latest(g) = self {
            g.set_max_key(max);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute the generalized harmonic number H_{n,theta}.
fn zeta(n: usize, theta: f64) -> f64 {
    let mut sum = 0.0;
    for i in 0..n {
        sum += 1.0 / ((i + 1) as f64).powf(theta);
    }
    sum
}

/// FNV-1a hash for scrambling Zipfian output.
fn fnv_hash(mut val: u64) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for _ in 0..8 {
        hash ^= val & 0xFF;
        hash = hash.wrapping_mul(0x100000001b3);
        val >>= 8;
    }
    hash
}

/// Format a YCSB key: `user{:010}`.
pub fn ycsb_key(index: usize) -> String {
    format!("user{:010}", index)
}
