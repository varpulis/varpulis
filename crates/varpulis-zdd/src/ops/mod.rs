//! ZDD operations
//!
//! This module provides set-theoretic operations on ZDDs.
//! Operations are implemented as methods on the Zdd struct.

mod difference;
mod intersection;
mod product;
mod union;

// Operations are implemented as impl blocks for Zdd in each module.
// The modules are included for their side effects (adding methods to Zdd).
