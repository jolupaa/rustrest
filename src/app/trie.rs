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
        self.find_candidates(method, segments).into_iter().next()
    }

    /// Returns every structurally matching route for `method` + path segments
    /// in route-precedence order: static branches before params before
    /// wildcards; exact method entries before `all()`; registration order for
    /// ties within the same branch.
    pub(crate) fn find_candidates(&self, method: &str, segments: &[&str]) -> Vec<usize> {
        let mut found = Vec::new();
        collect_candidates(&self.root, method, segments, &mut found);
        found
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

fn collect_candidates(node: &Node, method: &str, segments: &[&str], found: &mut Vec<usize>) {
    match segments.split_first() {
        None => {
            push_matching(&node.terminals, method, found);
            push_matching(&node.wildcards, method, found);
        }
        Some((head, rest)) => {
            if let Some(child) = node.statics.get(*head) {
                collect_candidates(child, method, rest, found);
            }
            if let Some(child) = node.param.as_deref() {
                collect_candidates(child, method, rest, found);
            }
            push_matching(&node.wildcards, method, found);
        }
    }
}

fn push_matching(entries: &[(String, usize)], method: &str, found: &mut Vec<usize>) {
    for (entry_method, index) in entries {
        if entry_method == method {
            found.push(*index);
        }
    }
    for (entry_method, index) in entries {
        if entry_method == METHOD_ALL {
            found.push(*index);
        }
    }
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
