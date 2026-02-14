//! Prediction Suffix Tree (PST) for Complex Event Forecasting
//!
//! Based on "Complex Event Forecasting with Prediction Suffix Trees"
//! (Alevizos, Artikis, Paliouras — arXiv:2109.00287)
//!
//! Provides variable-order Markov models that predict whether a partially-matched
//! SASE+ pattern will complete, and when. Combined with the NFA to form a
//! Pattern Markov Chain (PMC).

mod markov_chain;
mod online;
mod pruning;
mod tree;

pub use markov_chain::{ForecastResult, PMCConfig, PatternMarkovChain, RunSnapshot};
pub use online::OnlinePSTLearner;
pub use pruning::PruningStrategy;
pub use tree::{PSTConfig, PredictionSuffixTree, SymbolId};
