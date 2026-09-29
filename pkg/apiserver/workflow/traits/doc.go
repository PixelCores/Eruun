// Package traits renders the fixed workload traits in spec.Traits into Kubernetes
// workloads. Typed processors return TraitResult values; explicit dispatch keeps
// the processing order and nested exclusions visible to the compiler and reader.
// Aggregation de-duplicates volumes and objects, rejects conflicting object
// definitions, and preserves the last non-nil value for singleton fields.
package traits
