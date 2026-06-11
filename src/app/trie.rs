//! Segment-trie index over the registered routes, so lookups walk the path
//! (O(path length)) instead of scanning every route. Matching prefers the most
//! specific branch — static > `:param` > trailing `*wildcard`, backtracking
//! when a branch dead-ends — and within one node an exact method beats an
//! `all()` registration; remaining ties go to the first-registered route.

use std::collections::HashMap;

use super::router::{METHOD_ALL, Segment};

#[derive(Default)]
pub(crate) struct RouteIndex {
    root: Node,
}

#[derive(Default)]
struct Node {
    statics: HashMap<String, Node>,
    param: Option<Box<Node>>,
    /// Routes whose pattern ends exactly at this node: (method, registration index).
    terminals: Vec<(String, usize)>,
    /// Trailing-wildcard routes anchored at this node; they absorb any
    /// remaining suffix, including the empty one.
    wildcards: Vec<(String, usize)>,
}

impl RouteIndex {
    /// Indexes `(method, pattern)` pairs by registration order. Patterns with
    /// a non-trailing wildcard are unmatchable (mirroring `match_pattern`) and
    /// are skipped.
    pub(crate) fn build<'a>(patterns: impl Iterator<Item = (&'a str, &'a [Segment])>) -> Self {
        let mut root = Node::default();
        'routes: for (index, (method, pattern)) in patterns.enumerate() {
            let mut node = &mut root;
            for (position, segment) in pattern.iter().enumerate() {
                match segment {
                    Segment::Static(s) => node = node.statics.entry(s.clone()).or_default(),
                    Segment::Param(_) => node = node.param.get_or_insert_with(Default::default),
                    Segment::Wildcard(_) => {
                        if position == pattern.len() - 1 {
                            node.wildcards.push((method.to_string(), index));
                        }
                        continue 'routes;
                    }
                }
            }
            node.terminals.push((method.to_string(), index));
        }
        Self { root }
    }

    /// Returns the registration index of the best route for `method` + path
    /// segments, or `None` when nothing matches.
    pub(crate) fn find(&self, method: &str, segments: &[&str]) -> Option<usize> {
        find_in(&self.root, method, segments)
    }

    /// Returns `(registration index, method)` for every route whose pattern
    /// matches the path segments regardless of method, in registration order.
    /// Backs the `Allow` header for 405/OPTIONS responses.
    pub(crate) fn matching_methods(&self, segments: &[&str]) -> Vec<(usize, String)> {
        let mut found = Vec::new();
        collect_methods(&self.root, segments, &mut found);
        found.sort_by_key(|(index, _)| *index);
        found
    }
}

fn find_in(node: &Node, method: &str, segments: &[&str]) -> Option<usize> {
    match segments.split_first() {
        None => pick(&node.terminals, method).or_else(|| pick(&node.wildcards, method)),
        Some((head, rest)) => node
            .statics
            .get(*head)
            .and_then(|child| find_in(child, method, rest))
            .or_else(|| {
                node.param
                    .as_deref()
                    .and_then(|child| find_in(child, method, rest))
            })
            .or_else(|| pick(&node.wildcards, method)),
    }
}

/// First entry registered for exactly `method`, falling back to `all()`.
fn pick(entries: &[(String, usize)], method: &str) -> Option<usize> {
    entries
        .iter()
        .find(|(m, _)| m == method)
        .or_else(|| entries.iter().find(|(m, _)| m == METHOD_ALL))
        .map(|(_, index)| *index)
}

fn collect_methods(node: &Node, segments: &[&str], found: &mut Vec<(usize, String)>) {
    for (method, index) in &node.wildcards {
        found.push((*index, method.clone()));
    }
    match segments.split_first() {
        None => {
            for (method, index) in &node.terminals {
                found.push((*index, method.clone()));
            }
        }
        Some((head, rest)) => {
            if let Some(child) = node.statics.get(*head) {
                collect_methods(child, rest, found);
            }
            if let Some(child) = &node.param {
                collect_methods(child, rest, found);
            }
        }
    }
}
