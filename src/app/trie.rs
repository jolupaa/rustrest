//! Segment-trie index over the registered routes. Lookup is iterative and its
//! work scales with path length plus the compatible trie branches and matching
//! entries explored; adversarial overlapping branches can approach the index
//! size. Matching prefers the most specific branch — static > `:param` >
//! trailing `*wildcard`, backtracking when a branch dead-ends — and within one
//! node an exact method beats an `all()` registration; remaining ties go to the
//! first-registered route.

use std::borrow::Cow;
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

    /// Returns every structurally matching route for `method` + path segments
    /// in route-precedence order: static branches before params before
    /// wildcards; exact method entries before `all()`; registration order for
    /// ties within the same branch.
    pub(crate) fn find_candidates(&self, method: &str, segments: &[Cow<'_, str>]) -> Vec<usize> {
        let mut found = Vec::new();
        collect_candidates(&self.root, method, segments, &mut found);
        found
    }

    /// Returns `(registration index, method)` for every route whose pattern
    /// matches the path segments regardless of method, in registration order.
    /// Backs the `Allow` header for 405/OPTIONS responses.
    pub(crate) fn matching_methods(&self, segments: &[Cow<'_, str>]) -> Vec<(usize, String)> {
        let mut found = Vec::new();
        collect_methods(&self.root, segments, &mut found);
        found.sort_by_key(|(index, _)| *index);
        found
    }
}

fn collect_candidates(
    node: &Node,
    method: &str,
    segments: &[Cow<'_, str>],
    found: &mut Vec<usize>,
) {
    enum Work<'a> {
        Visit(&'a Node, usize),
        Wildcards(&'a Node),
    }

    let mut work = vec![Work::Visit(node, 0)];
    while let Some(next) = work.pop() {
        match next {
            Work::Wildcards(node) => push_matching(&node.wildcards, method, found),
            Work::Visit(node, index) if index == segments.len() => {
                push_matching(&node.terminals, method, found);
                push_matching(&node.wildcards, method, found);
            }
            Work::Visit(node, index) => {
                // Push in reverse so processing remains static > param >
                // wildcard without recursive stack growth on deep paths.
                work.push(Work::Wildcards(node));
                if let Some(child) = node.param.as_deref() {
                    work.push(Work::Visit(child, index + 1));
                }
                if let Some(child) = node.statics.get(segments[index].as_ref()) {
                    work.push(Work::Visit(child, index + 1));
                }
            }
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

fn collect_methods(node: &Node, segments: &[Cow<'_, str>], found: &mut Vec<(usize, String)>) {
    let mut work = vec![(node, 0_usize)];
    while let Some((node, segment_index)) = work.pop() {
        for (method, index) in &node.wildcards {
            found.push((*index, method.clone()));
        }
        if segment_index == segments.len() {
            for (method, index) in &node.terminals {
                found.push((*index, method.clone()));
            }
        } else {
            // Reverse push order preserves static-before-param traversal.
            if let Some(child) = &node.param {
                work.push((child, segment_index + 1));
            }
            if let Some(child) = node.statics.get(segments[segment_index].as_ref()) {
                work.push((child, segment_index + 1));
            }
        }
    }
}
