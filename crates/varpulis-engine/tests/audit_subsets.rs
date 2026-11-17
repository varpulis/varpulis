//! `.subsets()` emits one match per non-empty subset of the closure, and
//! each match carries its own subset.
//!
//! The count was right and the content was not: every one of the seven
//! matches for three closure events carried all three, so `count(b)` was 3
//! seven times over, which the guide admitted.

mod common;

use common::{at, feed};
use varpulis_engine::Program;

#[test]
fn each_subset_match_carries_its_own_subset() {
    let program = r"
event A:
    k: str
event B:
    k: str
    x: int
event C:
    k: str
stream S = A -> all B as b -> C
    .within(1h)
    .subsets()
    .emit(n: count(b), xs: collect(b.x), last: b.x)
";
    let mut p = Program::compile(program).unwrap();
    assert!(feed(&mut p, "A", &at("10:00:00", r#""k": "a""#)).is_empty());
    for (i, x) in [1, 2, 3].iter().enumerate() {
        assert!(feed(
            &mut p,
            "B",
            &at(
                &format!("10:00:0{}", i + 1),
                &format!(r#""k": "a", "x": {x}"#)
            )
        )
        .is_empty());
    }
    let out = feed(&mut p, "C", &at("10:00:09", r#""k": "a""#));
    assert_eq!(out.len(), 7, "{out:#?}");
    let mut subsets: Vec<Vec<i64>> = out
        .iter()
        .map(|e| {
            let xs: Vec<i64> = e["xs"]
                .as_array()
                .unwrap_or_else(|| panic!("xs must be an array: {e}"))
                .iter()
                .map(|v| v.as_i64().unwrap())
                .collect();
            assert_eq!(
                e["n"].as_i64().unwrap() as usize,
                xs.len(),
                "count(b) is the size of the subset: {e}"
            );
            assert_eq!(
                e["last"],
                *xs.last().unwrap(),
                "b is the last of the subset: {e}"
            );
            xs
        })
        .collect();
    subsets.sort();
    assert_eq!(
        subsets,
        vec![
            vec![1],
            vec![1, 2],
            vec![1, 2, 3],
            vec![1, 3],
            vec![2],
            vec![2, 3],
            vec![3]
        ]
    );
}
