//! context-graph subsystem for `InsiderTrader`.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "context_graph";

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Supported graph entity kinds.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NodeType {
    /// Canonical tradable instrument.
    Instrument,
    /// Issuer/company.
    Issuer,
    /// News article.
    NewsItem,
    /// Deduplicated news event.
    NewsCluster,
    /// Metric definition.
    Metric,
    /// Strategy definition.
    Strategy,
    /// Portfolio position.
    Position,
    /// Order or fill.
    Order,
    /// Executed fill.
    Fill,
    /// Macro or event node.
    Event,
    /// Sector classification.
    Sector,
    /// Industry classification.
    Industry,
    /// Benchmark index.
    Index,
    /// Exchange-traded fund.
    Etf,
    /// Currency instrument or currency entity.
    Currency,
    /// Commodity entity.
    Commodity,
    /// Country or jurisdiction.
    Country,
    /// Macroeconomic series.
    MacroSeries,
    /// Person or decision-maker.
    Person,
    /// Model artifact.
    Model,
    /// Portfolio aggregate.
    Portfolio,
    /// Research experiment/run.
    Experiment,
}

/// A graph node with stable identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    /// Globally stable node ID.
    pub id: String,
    /// Entity type.
    pub node_type: NodeType,
    /// Human-readable label.
    pub label: String,
}

/// A directed, typed relationship.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Edge {
    /// Source node ID.
    pub from: String,
    /// Relationship vocabulary.
    pub relation: String,
    /// Destination node ID.
    pub to: String,
}

/// Half-open millisecond interval used for valid-time and knowledge-time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeInterval {
    /// Inclusive start timestamp.
    pub start_ms: i64,
    /// Exclusive end timestamp; `None` means open-ended.
    pub end_ms: Option<i64>,
}

impl TimeInterval {
    /// Creates and validates an interval.
    ///
    /// # Errors
    /// Returns [`GraphError::InvalidInterval`] for negative or reversed bounds.
    pub fn new(start_ms: i64, end_ms: Option<i64>) -> Result<Self, GraphError> {
        if start_ms < 0 || end_ms.is_some_and(|end| end <= start_ms) {
            return Err(GraphError::InvalidInterval);
        }
        Ok(Self { start_ms, end_ms })
    }

    fn contains(self, timestamp_ms: i64) -> bool {
        timestamp_ms >= self.start_ms && self.end_ms.is_none_or(|end| timestamp_ms < end)
    }
}

/// Source and confidence metadata retained on every graph fact.
#[derive(Clone, Debug, PartialEq)]
pub struct Provenance {
    /// Stable source/provider identity.
    pub source: String,
    /// Immutable source artifact or event identity.
    pub artifact_id: String,
    /// Source confidence in `[0, 1]`.
    pub confidence: f64,
}

/// Versioned node fact with valid-time and knowledge-time intervals.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeFact {
    /// Node identity and display metadata.
    pub node: Node,
    /// When the fact is true in the modeled world.
    pub valid: TimeInterval,
    /// When the system knew this version.
    pub known: TimeInterval,
    /// Source provenance and confidence.
    pub provenance: Provenance,
}

/// Versioned edge fact with valid-time and knowledge-time intervals.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeFact {
    /// Relationship identity and vocabulary.
    pub edge: Edge,
    /// When the relationship is true in the modeled world.
    pub valid: TimeInterval,
    /// When the system knew this version.
    pub known: TimeInterval,
    /// Source provenance and confidence.
    pub provenance: Provenance,
}

/// Graph mutation/query failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphError {
    /// Node identity or relationship is blank.
    InvalidIdentity,
    /// An edge references an unknown node.
    MissingNode,
    /// A node ID was already defined with different metadata.
    ConflictingNode,
    /// A valid-time or knowledge-time interval is invalid.
    InvalidInterval,
    /// Provenance metadata is blank, non-finite, or outside `[0, 1]`.
    InvalidProvenance,
}

/// A versioned embedding stored for one immutable graph representation.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddingRecord {
    /// Stable graph node identity.
    pub node_id: String,
    /// Hash of the text/content represented by the vector.
    pub content_hash: String,
    /// Embedding model family.
    pub model: String,
    /// Model version or artifact revision.
    pub model_version: String,
    /// Vector dimensions.
    pub dimensions: usize,
    /// L2-normalized vector values.
    pub vector: Vec<f32>,
    /// Creation timestamp in milliseconds.
    pub created_at_ms: i64,
}

/// Complete immutable input used to build an embedding-index generation.
/// Records are validated before a generation can become visible.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddingIndexSnapshot {
    /// Embedding model family.
    pub model: String,
    /// Embedding model revision.
    pub model_version: String,
    /// Vector dimensions.
    pub dimensions: usize,
    /// Records in deterministic node-id order after source serialization.
    pub records: Vec<EmbeddingRecord>,
}

/// Embedding index validation/search failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EmbeddingError {
    /// Model identity or node/content identity is blank.
    InvalidIdentity,
    /// Vector dimensions or values are invalid.
    InvalidVector,
    /// The index rejects a vector from a different model/version/dimension.
    ModelMismatch,
    /// Requested search vector does not match the index.
    QueryMismatch,
}

/// One vector-search result with an explainable similarity score.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddingHit {
    /// Stable graph node identity.
    pub node_id: String,
    /// Cosine similarity of normalized vectors.
    pub score: f64,
}

/// Bounded model-versioned embedding index.
#[derive(Clone, Debug)]
pub struct EmbeddingIndex {
    model: String,
    model_version: String,
    dimensions: usize,
    records: BTreeMap<String, EmbeddingRecord>,
}

impl EmbeddingIndex {
    /// Creates an empty index for exactly one model/version/dimension tuple.
    ///
    /// # Errors
    /// Returns [`EmbeddingError::InvalidIdentity`] for blank model metadata or
    /// a zero dimension.
    pub fn new(
        model: impl Into<String>,
        model_version: impl Into<String>,
        dimensions: usize,
    ) -> Result<Self, EmbeddingError> {
        let model = model.into();
        let model_version = model_version.into();
        if model.trim().is_empty() || model_version.trim().is_empty() || dimensions == 0 {
            return Err(EmbeddingError::InvalidIdentity);
        }
        Ok(Self {
            model,
            model_version,
            dimensions,
            records: BTreeMap::new(),
        })
    }

    /// Inserts or replaces one node vector after normalizing it atomically.
    ///
    /// # Errors
    /// Returns [`EmbeddingError::ModelMismatch`] for a different model tuple
    /// and [`EmbeddingError::InvalidVector`] for malformed values.
    #[allow(clippy::cast_possible_truncation)]
    pub fn upsert(&mut self, mut record: EmbeddingRecord) -> Result<(), EmbeddingError> {
        if record.node_id.trim().is_empty()
            || record.content_hash.trim().is_empty()
            || record.model != self.model
            || record.model_version != self.model_version
            || record.dimensions != self.dimensions
            || record.created_at_ms < 0
            || record.vector.len() != self.dimensions
            || record.vector.iter().any(|value| !value.is_finite())
        {
            return Err(
                if record.model != self.model
                    || record.model_version != self.model_version
                    || record.dimensions != self.dimensions
                {
                    EmbeddingError::ModelMismatch
                } else {
                    EmbeddingError::InvalidVector
                },
            );
        }
        let norm = record
            .vector
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            .sqrt();
        if !norm.is_finite() || norm <= f64::EPSILON {
            return Err(EmbeddingError::InvalidVector);
        }
        for value in &mut record.vector {
            *value = (f64::from(*value) / norm) as f32;
        }
        self.records.insert(record.node_id.clone(), record);
        Ok(())
    }

    /// Builds a fresh index generation from a complete snapshot. No partially
    /// validated generation is returned to callers.
    ///
    /// # Errors
    /// Returns the first model, identity, dimension, or numeric validation
    /// error encountered while constructing the generation.
    pub fn from_snapshot(snapshot: EmbeddingIndexSnapshot) -> Result<Self, EmbeddingError> {
        let mut index = Self::new(snapshot.model, snapshot.model_version, snapshot.dimensions)?;
        for record in snapshot.records {
            index.upsert(record)?;
        }
        Ok(index)
    }

    /// Exports a deterministic snapshot suitable for a rebuild or checkpoint.
    #[must_use]
    pub fn snapshot(&self) -> EmbeddingIndexSnapshot {
        EmbeddingIndexSnapshot {
            model: self.model.clone(),
            model_version: self.model_version.clone(),
            dimensions: self.dimensions,
            records: self.records.values().cloned().collect(),
        }
    }

    /// Searches normalized vectors and returns deterministic top-k results.
    ///
    /// # Errors
    /// Returns [`EmbeddingError::QueryMismatch`] when the query dimensions or
    /// values are invalid.
    pub fn search(&self, query: &[f32], limit: usize) -> Result<Vec<EmbeddingHit>, EmbeddingError> {
        if query.len() != self.dimensions || query.iter().any(|value| !value.is_finite()) {
            return Err(EmbeddingError::QueryMismatch);
        }
        let norm = query
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            .sqrt();
        if !norm.is_finite() || norm <= f64::EPSILON {
            return Err(EmbeddingError::QueryMismatch);
        }
        let mut hits = self
            .records
            .values()
            .map(|record| EmbeddingHit {
                node_id: record.node_id.clone(),
                score: record
                    .vector
                    .iter()
                    .zip(query)
                    .map(|(left, right)| f64::from(*left) * (f64::from(*right) / norm))
                    .sum::<f64>(),
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    /// Returns the index's model tuple for diagnostics and compatibility checks.
    #[must_use]
    pub fn model_tuple(&self) -> (&str, &str, usize) {
        (&self.model, &self.model_version, self.dimensions)
    }

    /// Returns the number of indexed vectors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns whether the index contains no vectors.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// In-memory authoritative graph projection.
#[derive(Default)]
pub struct Graph {
    nodes: BTreeMap<String, Node>,
    edges: BTreeSet<Edge>,
    node_facts: BTreeMap<String, Vec<NodeFact>>,
    edge_facts: BTreeMap<Edge, Vec<EdgeFact>>,
    embedding_index: Option<EmbeddingIndex>,
}

/// Bounded hybrid retrieval request.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalQuery {
    /// User text used for deterministic lexical matching.
    pub text: String,
    /// Optional vector from the same model tuple as the supplied index.
    pub embedding: Option<Vec<f32>>,
    /// Optional graph root used to add bounded relationship evidence.
    pub graph_root: Option<String>,
    /// Maximum graph traversal depth for evidence paths.
    pub max_depth: usize,
}

/// Explainable hybrid result returned to terminal/LLM callers.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalHit {
    /// Stable graph node identity.
    pub node_id: String,
    /// Final versioned rank score.
    pub score: f64,
    /// Exact ID match component.
    pub exact_score: f64,
    /// Lexical label/ID overlap component.
    pub lexical_score: f64,
    /// Vector similarity component, if available.
    pub vector_score: f64,
    /// Bounded graph evidence path from the requested root.
    pub evidence_path: Vec<String>,
}

/// Hybrid retrieval failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetrievalError {
    /// Embedding index/query incompatibility.
    Embedding(EmbeddingError),
    /// A zero result limit is invalid.
    InvalidLimit,
}

impl Graph {
    /// Creates an empty graph.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            edges: BTreeSet::new(),
            node_facts: BTreeMap::new(),
            edge_facts: BTreeMap::new(),
            embedding_index: None,
        }
    }

    /// Installs the single model/version tuple allowed for this projection.
    /// Replacing an index is rejected when it would silently mix model
    /// versions; callers must explicitly create a fresh projection instead.
    ///
    /// # Errors
    /// Returns [`EmbeddingError::InvalidIdentity`] for invalid model metadata
    /// or [`EmbeddingError::ModelMismatch`] when an existing index differs.
    pub fn configure_embedding_index(
        &mut self,
        model: impl Into<String>,
        model_version: impl Into<String>,
        dimensions: usize,
    ) -> Result<(), EmbeddingError> {
        let next = EmbeddingIndex::new(model, model_version, dimensions)?;
        if let Some(existing) = &self.embedding_index
            && existing.model_tuple() != next.model_tuple()
        {
            return Err(EmbeddingError::ModelMismatch);
        }
        self.embedding_index = Some(next);
        Ok(())
    }

    /// Stores one validated, normalized embedding in the configured index.
    ///
    /// # Errors
    /// Returns [`EmbeddingError::InvalidIdentity`] when no index is configured
    /// and forwards validation errors from the configured index otherwise.
    pub fn upsert_embedding(&mut self, record: EmbeddingRecord) -> Result<(), EmbeddingError> {
        let Some(index) = self.embedding_index.as_mut() else {
            return Err(EmbeddingError::InvalidIdentity);
        };
        index.upsert(record)
    }

    /// Atomically replaces the rebuildable embedding projection. The new
    /// generation is fully validated before the current index is swapped.
    ///
    /// # Errors
    /// Returns an embedding validation error and leaves the current index
    /// unchanged when any snapshot record is invalid.
    pub fn replace_embeddings(
        &mut self,
        snapshot: EmbeddingIndexSnapshot,
    ) -> Result<(), EmbeddingError> {
        let next = EmbeddingIndex::from_snapshot(snapshot)?;
        self.embedding_index = Some(next);
        Ok(())
    }

    /// Returns the configured embedding model tuple, if semantic retrieval is enabled.
    #[must_use]
    pub fn embedding_model_tuple(&self) -> Option<(&str, &str, usize)> {
        self.embedding_index
            .as_ref()
            .map(EmbeddingIndex::model_tuple)
    }

    /// Inserts a node idempotently.
    ///
    /// # Errors
    /// Returns `GraphError` for blank identity or conflicting redefinition.
    pub fn upsert_node(&mut self, node: Node) -> Result<(), GraphError> {
        if node.id.trim().is_empty() || node.label.trim().is_empty() {
            return Err(GraphError::InvalidIdentity);
        }
        if let Some(existing) = self.nodes.get(&node.id) {
            if existing != &node {
                return Err(GraphError::ConflictingNode);
            }
            return Ok(());
        }
        let node_id = node.id.clone();
        self.nodes.insert(node_id, node.clone());
        let fact = NodeFact {
            node,
            valid: TimeInterval {
                start_ms: 0,
                end_ms: None,
            },
            known: TimeInterval {
                start_ms: 0,
                end_ms: None,
            },
            provenance: Provenance {
                source: String::from("legacy"),
                artifact_id: String::from("legacy"),
                confidence: 1.0,
            },
        };
        self.node_facts
            .entry(fact.node.id.clone())
            .or_default()
            .push(fact);
        Ok(())
    }

    /// Adds a versioned node fact without deleting prior knowledge.
    ///
    /// # Errors
    /// Returns [`GraphError`] for invalid identity, intervals, provenance, or
    /// a conflicting fact with the same identity/knowledge boundary.
    pub fn upsert_node_fact(&mut self, fact: NodeFact) -> Result<(), GraphError> {
        validate_node(&fact.node)?;
        validate_fact_metadata(fact.valid, fact.known, &fact.provenance)?;
        let versions = self.node_facts.entry(fact.node.id.clone()).or_default();
        if versions.iter().any(|existing| existing == &fact) {
            return Ok(());
        }
        if versions.iter().any(|existing| {
            existing.known.start_ms == fact.known.start_ms
                && existing.valid == fact.valid
                && existing.provenance.artifact_id == fact.provenance.artifact_id
                && existing.node != fact.node
        }) {
            return Err(GraphError::ConflictingNode);
        }
        for existing in versions.iter_mut() {
            if existing.valid == fact.valid
                && existing.known.start_ms < fact.known.start_ms
                && existing
                    .known
                    .end_ms
                    .is_none_or(|end| end > fact.known.start_ms)
            {
                existing.known.end_ms = Some(fact.known.start_ms);
            }
        }
        versions.push(fact);
        versions.sort_by_key(|version| (version.known.start_ms, version.valid.start_ms));
        if let Some(current) = versions.last().map(|version| version.node.clone()) {
            self.nodes.insert(current.id.clone(), current);
        }
        Ok(())
    }

    /// Adds a typed edge idempotently after validating both endpoints.
    ///
    /// # Errors
    /// Returns `GraphError` for blank relationships or missing endpoints.
    pub fn add_edge(&mut self, edge: Edge) -> Result<(), GraphError> {
        if edge.from.trim().is_empty()
            || edge.to.trim().is_empty()
            || edge.relation.trim().is_empty()
        {
            return Err(GraphError::InvalidIdentity);
        }
        if !self.nodes.contains_key(&edge.from) || !self.nodes.contains_key(&edge.to) {
            return Err(GraphError::MissingNode);
        }
        let edge_key = edge;
        self.edges.insert(edge_key.clone());
        let fact = EdgeFact {
            edge: edge_key.clone(),
            valid: TimeInterval {
                start_ms: 0,
                end_ms: None,
            },
            known: TimeInterval {
                start_ms: 0,
                end_ms: None,
            },
            provenance: Provenance {
                source: String::from("legacy"),
                artifact_id: String::from("legacy"),
                confidence: 1.0,
            },
        };
        self.edge_facts.entry(edge_key).or_default().push(fact);
        Ok(())
    }

    /// Adds a versioned relationship fact and retains prior versions.
    ///
    /// # Errors
    /// Returns [`GraphError`] when endpoints, intervals, or provenance are
    /// invalid, or when the same source boundary conflicts.
    pub fn add_edge_fact(&mut self, fact: EdgeFact) -> Result<(), GraphError> {
        validate_edge(&fact.edge)?;
        validate_fact_metadata(fact.valid, fact.known, &fact.provenance)?;
        if !self.nodes.contains_key(&fact.edge.from) || !self.nodes.contains_key(&fact.edge.to) {
            return Err(GraphError::MissingNode);
        }
        let versions = self.edge_facts.entry(fact.edge.clone()).or_default();
        if versions.iter().any(|existing| existing == &fact) {
            return Ok(());
        }
        for existing in versions.iter_mut() {
            if existing.valid == fact.valid
                && existing.known.start_ms < fact.known.start_ms
                && existing
                    .known
                    .end_ms
                    .is_none_or(|end| end > fact.known.start_ms)
            {
                existing.known.end_ms = Some(fact.known.start_ms);
            }
        }
        versions.push(fact.clone());
        versions.sort_by_key(|version| (version.known.start_ms, version.valid.start_ms));
        self.edges.insert(fact.edge);
        Ok(())
    }

    /// Returns node facts visible at valid time and knowledge time.
    #[must_use]
    pub fn nodes_at(&self, valid_at_ms: i64, known_at_ms: i64) -> Vec<&NodeFact> {
        self.node_facts
            .values()
            .flat_map(|versions| versions.iter())
            .filter(|fact| fact.valid.contains(valid_at_ms) && fact.known.contains(known_at_ms))
            .collect()
    }

    /// Returns edge facts visible at valid time and knowledge time.
    #[must_use]
    pub fn edges_at(&self, valid_at_ms: i64, known_at_ms: i64) -> Vec<&EdgeFact> {
        self.edge_facts
            .values()
            .flat_map(|versions| versions.iter())
            .filter(|fact| fact.valid.contains(valid_at_ms) && fact.known.contains(known_at_ms))
            .collect()
    }

    /// Returns retained historical fact counts for diagnostics.
    #[must_use]
    pub fn fact_counts(&self) -> (usize, usize) {
        (
            self.node_facts.values().map(Vec::len).sum(),
            self.edge_facts.values().map(Vec::len).sum(),
        )
    }

    /// Returns a node by stable ID.
    #[must_use]
    pub fn node(&self, id: &str) -> Option<&Node> {
        self.nodes.get(id)
    }

    /// Traverses outgoing and incoming edges up to a bounded depth.
    #[must_use]
    pub fn neighborhood(&self, root: &str, max_depth: usize) -> Vec<&Node> {
        if !self.nodes.contains_key(root) {
            return Vec::new();
        }
        let mut queue = VecDeque::from([(root.to_owned(), 0_usize)]);
        let mut visited = BTreeSet::from([root.to_owned()]);
        let mut result = Vec::new();
        while let Some((current, depth)) = queue.pop_front() {
            if let Some(node) = self.nodes.get(&current) {
                result.push(node);
            }
            if depth >= max_depth {
                continue;
            }
            for edge in &self.edges {
                let next = if edge.from == current {
                    Some(&edge.to)
                } else if edge.to == current {
                    Some(&edge.from)
                } else {
                    None
                };
                if let Some(next) = next
                    && visited.insert(next.clone())
                {
                    queue.push_back((next.clone(), depth + 1));
                }
            }
        }
        result
    }

    /// Searches exact IDs, labels, graph evidence, and an optional versioned
    /// embedding index. Missing vector support naturally falls back to exact
    /// and lexical ranking without touching trading state.
    ///
    /// # Errors
    /// Returns [`RetrievalError::InvalidLimit`] for zero limits or
    /// [`RetrievalError::Embedding`] when the configured index itself rejects
    /// an operation; an incompatible optional query vector falls back to
    /// deterministic non-vector ranking for that request.
    #[allow(clippy::cast_precision_loss)]
    pub fn hybrid_search(
        &self,
        index: Option<&EmbeddingIndex>,
        query: &RetrievalQuery,
        limit: usize,
    ) -> Result<Vec<RetrievalHit>, RetrievalError> {
        if limit == 0 {
            return Err(RetrievalError::InvalidLimit);
        }
        let index = index.or(self.embedding_index.as_ref());
        let vector_scores = match (index, query.embedding.as_deref()) {
            (Some(index), Some(embedding)) => match index.search(embedding, self.nodes.len()) {
                Ok(hits) => hits
                    .into_iter()
                    .map(|hit| (hit.node_id, hit.score))
                    .collect::<BTreeMap<_, _>>(),
                // A bad caller-supplied vector must not make deterministic
                // exact/lexical/graph retrieval unavailable. Model/index
                // integrity errors remain explicit; only this request's
                // optional vector component is dropped.
                Err(EmbeddingError::QueryMismatch) => BTreeMap::new(),
                Err(error) => return Err(RetrievalError::Embedding(error)),
            },
            _ => BTreeMap::new(),
        };
        let tokens = query
            .text
            .split_whitespace()
            .map(str::to_lowercase)
            .filter(|token| !token.is_empty())
            .collect::<BTreeSet<_>>();
        let root = query.graph_root.as_deref();
        let mut results = self
            .nodes
            .values()
            .map(|node| {
                let exact_score = f64::from(u8::from(root.is_some_and(|root| root == node.id)));
                let lexical_score = if tokens.is_empty() {
                    0.0
                } else {
                    let haystack = format!("{} {}", node.id, node.label).to_lowercase();
                    tokens
                        .iter()
                        .filter(|token| {
                            haystack
                                .split_whitespace()
                                .any(|word| word == token.as_str())
                        })
                        .count() as f64
                        / tokens.len() as f64
                };
                let evidence_path = root
                    .and_then(|root| self.path_between(root, &node.id, query.max_depth))
                    .unwrap_or_default();
                let graph_score = if evidence_path.is_empty() {
                    0.0
                } else {
                    1.0 / evidence_path.len() as f64
                };
                let vector_score = vector_scores.get(&node.id).copied().unwrap_or(0.0);
                let score =
                    exact_score * 1_000.0 + lexical_score * 10.0 + vector_score + graph_score;
                RetrievalHit {
                    node_id: node.id.clone(),
                    score,
                    exact_score,
                    lexical_score,
                    vector_score,
                    evidence_path,
                }
            })
            .collect::<Vec<_>>();
        results.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        results.truncate(limit);
        Ok(results)
    }

    fn path_between(&self, root: &str, target: &str, max_depth: usize) -> Option<Vec<String>> {
        if !self.nodes.contains_key(root) || !self.nodes.contains_key(target) {
            return None;
        }
        let mut queue = VecDeque::from([(root.to_owned(), 0_usize)]);
        let mut parents = BTreeMap::from([(root.to_owned(), None)]);
        while let Some((current, depth)) = queue.pop_front() {
            if current == target {
                let mut path = Vec::new();
                let mut cursor = Some(current);
                while let Some(node_id) = cursor {
                    cursor = parents.get(&node_id).and_then(Option::clone);
                    path.push(node_id);
                }
                path.reverse();
                return Some(path);
            }
            if depth >= max_depth {
                continue;
            }
            for edge in &self.edges {
                let next = if edge.from == current {
                    &edge.to
                } else if edge.to == current {
                    &edge.from
                } else {
                    continue;
                };
                if !parents.contains_key(next) {
                    parents.insert(next.clone(), Some(current.clone()));
                    queue.push_back((next.clone(), depth + 1));
                }
            }
        }
        None
    }

    /// Returns the number of nodes and edges for health diagnostics.
    #[must_use]
    pub fn counts(&self) -> (usize, usize) {
        (self.nodes.len(), self.edges.len())
    }
}

fn validate_node(node: &Node) -> Result<(), GraphError> {
    if node.id.trim().is_empty() || node.label.trim().is_empty() {
        return Err(GraphError::InvalidIdentity);
    }
    Ok(())
}

fn validate_edge(edge: &Edge) -> Result<(), GraphError> {
    if edge.from.trim().is_empty() || edge.to.trim().is_empty() || edge.relation.trim().is_empty() {
        return Err(GraphError::InvalidIdentity);
    }
    Ok(())
}

fn validate_fact_metadata(
    valid: TimeInterval,
    known: TimeInterval,
    provenance: &Provenance,
) -> Result<(), GraphError> {
    TimeInterval::new(valid.start_ms, valid.end_ms)?;
    TimeInterval::new(known.start_ms, known.end_ms)?;
    if provenance.source.trim().is_empty()
        || provenance.artifact_id.trim().is_empty()
        || !provenance.confidence.is_finite()
        || !(0.0..=1.0).contains(&provenance.confidence)
    {
        return Err(GraphError::InvalidProvenance);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        Edge, EmbeddingIndex, EmbeddingIndexSnapshot, EmbeddingRecord, Graph, Node, NodeFact,
        NodeType, Provenance, RetrievalQuery, SUBSYSTEM_ID, TimeInterval,
    };

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn graph_upserts_idempotently_and_traverses_bounded_context() {
        let mut graph = Graph::new();
        assert!(
            graph
                .upsert_node(Node {
                    id: "instrument:AAPL".into(),
                    node_type: NodeType::Instrument,
                    label: "AAPL".into()
                })
                .is_ok()
        );
        assert!(
            graph
                .upsert_node(Node {
                    id: "issuer:apple".into(),
                    node_type: NodeType::Issuer,
                    label: "Apple".into()
                })
                .is_ok()
        );
        assert!(
            graph
                .upsert_node(Node {
                    id: "news:1".into(),
                    node_type: NodeType::NewsItem,
                    label: "Earnings".into()
                })
                .is_ok()
        );
        assert!(
            graph
                .add_edge(Edge {
                    from: "instrument:AAPL".into(),
                    relation: "ISSUED_BY".into(),
                    to: "issuer:apple".into()
                })
                .is_ok()
        );
        assert!(
            graph
                .add_edge(Edge {
                    from: "issuer:apple".into(),
                    relation: "MENTIONS".into(),
                    to: "news:1".into()
                })
                .is_ok()
        );
        assert_eq!(graph.neighborhood("instrument:AAPL", 1).len(), 2);
        assert_eq!(graph.neighborhood("instrument:AAPL", 2).len(), 3);
        assert_eq!(graph.counts(), (3, 2));
    }

    #[test]
    fn point_in_time_facts_retain_corrections_and_filter_by_knowledge() {
        let mut graph = Graph::new();
        let first = NodeFact {
            node: Node {
                id: "issuer:acme".into(),
                node_type: NodeType::Issuer,
                label: "Acme Corp".into(),
            },
            valid: TimeInterval::new(1_000, Some(2_000)).unwrap_or(TimeInterval {
                start_ms: 1_000,
                end_ms: Some(2_000),
            }),
            known: TimeInterval::new(1_100, None).unwrap_or(TimeInterval {
                start_ms: 1_100,
                end_ms: None,
            }),
            provenance: Provenance {
                source: "filing".into(),
                artifact_id: "filing-v1".into(),
                confidence: 1.0,
            },
        };
        assert!(graph.upsert_node_fact(first).is_ok());
        let mut corrected = graph.nodes_at(1_500, 1_200).first().map_or_else(
            || NodeFact {
                node: Node {
                    id: "issuer:acme".into(),
                    node_type: NodeType::Issuer,
                    label: "Acme Corp".into(),
                },
                valid: TimeInterval::new(1_000, Some(2_000)).unwrap_or(TimeInterval {
                    start_ms: 1_000,
                    end_ms: Some(2_000),
                }),
                known: TimeInterval::new(1_100, None).unwrap_or(TimeInterval {
                    start_ms: 1_100,
                    end_ms: None,
                }),
                provenance: Provenance {
                    source: "filing".into(),
                    artifact_id: "filing-v1".into(),
                    confidence: 1.0,
                },
            },
            |fact| (*fact).clone(),
        );
        corrected.node.label = "Acme Corporation".into();
        corrected.known = TimeInterval::new(1_300, None).unwrap_or(TimeInterval {
            start_ms: 1_300,
            end_ms: None,
        });
        corrected.provenance.artifact_id = "filing-v2".into();
        assert!(graph.upsert_node_fact(corrected).is_ok());
        assert_eq!(graph.fact_counts().0, 2);
        assert_eq!(graph.nodes_at(1_500, 1_200).len(), 1);
        assert_eq!(graph.nodes_at(1_500, 1_200)[0].node.label, "Acme Corp");
        assert_eq!(
            graph.nodes_at(1_500, 1_400)[0].node.label,
            "Acme Corporation"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn versioned_embeddings_reject_mixed_models_and_hybrid_search_has_evidence() {
        let mut graph = Graph::new();
        assert!(
            graph
                .upsert_node(Node {
                    id: "instrument:AAPL".into(),
                    node_type: NodeType::Instrument,
                    label: "AAPL".into(),
                })
                .is_ok()
        );
        assert!(
            graph
                .upsert_node(Node {
                    id: "news:earnings".into(),
                    node_type: NodeType::NewsItem,
                    label: "Apple earnings".into(),
                })
                .is_ok()
        );
        assert!(
            graph
                .add_edge(Edge {
                    from: "instrument:AAPL".into(),
                    relation: "MENTIONS".into(),
                    to: "news:earnings".into(),
                })
                .is_ok()
        );
        let mut index = EmbeddingIndex::new("embed", "v1", 2).ok();
        assert!(index.as_mut().is_some_and(|index| {
            index
                .upsert(EmbeddingRecord {
                    node_id: "news:earnings".into(),
                    content_hash: "hash-1".into(),
                    model: "embed".into(),
                    model_version: "v1".into(),
                    dimensions: 2,
                    vector: vec![2.0, 0.0],
                    created_at_ms: 1,
                })
                .is_ok()
        }));
        assert!(index.as_mut().is_some_and(|index| {
            index
                .upsert(EmbeddingRecord {
                    node_id: "instrument:AAPL".into(),
                    content_hash: "hash-2".into(),
                    model: "other".into(),
                    model_version: "v1".into(),
                    dimensions: 2,
                    vector: vec![1.0, 0.0],
                    created_at_ms: 1,
                })
                .is_err()
        }));
        let hits = graph
            .hybrid_search(
                index.as_ref(),
                &RetrievalQuery {
                    text: "earnings".into(),
                    embedding: Some(vec![1.0, 0.0]),
                    graph_root: Some("instrument:AAPL".into()),
                    max_depth: 1,
                },
                2,
            )
            .ok();
        assert!(hits.is_some_and(|hits| hits.iter().any(|hit| {
            hit.node_id == "news:earnings"
                && hit.evidence_path == ["instrument:AAPL", "news:earnings"]
        })));
        assert!(graph.configure_embedding_index("embed", "v1", 2).is_ok());
        assert!(
            graph
                .upsert_embedding(EmbeddingRecord {
                    node_id: "news:earnings".into(),
                    content_hash: "hash-1".into(),
                    model: "embed".into(),
                    model_version: "v1".into(),
                    dimensions: 2,
                    vector: vec![2.0, 0.0],
                    created_at_ms: 1,
                })
                .is_ok()
        );
        let internal_hits = graph.hybrid_search(
            None,
            &RetrievalQuery {
                text: "unrelated".into(),
                embedding: Some(vec![1.0, 0.0]),
                graph_root: None,
                max_depth: 0,
            },
            1,
        );
        assert!(internal_hits.is_ok_and(|hits| hits[0].vector_score > 0.9));
        let previous = graph.embedding_index.as_ref().map(EmbeddingIndex::snapshot);
        assert!(
            graph
                .replace_embeddings(EmbeddingIndexSnapshot {
                    model: "embed".into(),
                    model_version: "v2".into(),
                    dimensions: 2,
                    records: vec![EmbeddingRecord {
                        node_id: "news:earnings".into(),
                        content_hash: "bad".into(),
                        model: "wrong".into(),
                        model_version: "v2".into(),
                        dimensions: 2,
                        vector: vec![1.0, 0.0],
                        created_at_ms: 1,
                    }],
                })
                .is_err()
        );
        assert_eq!(
            graph.embedding_index.as_ref().map(EmbeddingIndex::snapshot),
            previous
        );
    }
}
