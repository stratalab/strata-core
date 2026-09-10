//! Public inference API vocabulary.

pub use crate::registry::{ModelInfo, ModelTask};
pub use crate::runtime::{
    EmbedRequest, EmbedResponse, EmbedRuntimeOutcome, InferenceCapability, InferenceRuntime,
    InferenceRuntimeConfig, ModelCacheStatus, PullModelOutput, RankRequest, RankResponse,
    RankRuntimeOutcome,
};
pub use crate::wire::{
    ChatChoice, ChatMessage, ChatRequest, ChatResponse, EmbedInput, EmbeddingItem,
    EmbeddingsRequest, EmbeddingsResponse, FinishReason, InputType, Mirostat, ModelConfig, Pooling,
    RerankRequest, RerankResponse, RerankResult, ResponseFormat, Role, Usage,
};
pub use crate::{GenerateRequest, GenerateResponse, InferenceError, ProviderKind, StopReason};
