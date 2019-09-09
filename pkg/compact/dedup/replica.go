package dedup

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const (
	AggReplicaLabel = "_agg_replica_"
	ResolutionLabel = "resolution"
)

// Replica defines a group of blocks from same global configs and resolution.
// Ex, replica{name=r0,labels=[cluster=c0, shard=s0,resolution=0],blocks=[b0, b1]}
type Replica struct {
	Name   string            // the value specified by replica label
	Labels map[string]string // includes resolution and global configs
	Blocks []*metadata.Meta  // the underlying blocks
}

func NewReplica(name string, labels map[string]string, blocks []*metadata.Meta) *Replica {
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].MinTime < blocks[j].MinTime
	})
	return &Replica{
		Name:   name,
		Labels: labels,
		Blocks: blocks,
	}
}

func (r *Replica) MinTime() int64 {
	if len(r.Blocks) == 0 {
		return -1
	}
	return r.Blocks[0].MinTime
}

func (r *Replica) MaxTime() int64 {
	if len(r.Blocks) == 0 {
		return -1
	}
	return r.Blocks[len(r.Blocks)-1].MaxTime
}

func (r *Replica) Group() string {
	return replicaGroup(r.Labels)
}

func (r *Replica) isAggReplica() bool {
	return r.Name == AggReplicaLabel
}

func (r *Replica) Resolution() (int64, error) {
	if _, ok := r.Labels[ResolutionLabel]; !ok {
		return -1, fmt.Errorf("no resolution found in replica group")
	}
	res, err := strconv.ParseInt(r.Labels[ResolutionLabel], 10, 64)
	if err != nil {
		return -1, err
	}
	return res, nil
}

type Replicas []*Replica

// Group blocks by their global configs and resolution into different replicas.
func NewReplicas(replicaLabelName string, blocks []*metadata.Meta) (Replicas, error) {
	m := make(map[string]map[string][]*metadata.Meta, 0)
	groupLabels := make(map[string]map[string]string)
	for _, b := range blocks {
		name, ok := b.Thanos.Labels[replicaLabelName]
		if !ok {
			return nil, errors.Errorf("not found replica label '%s' on block: %s", replicaLabelName, b.ULID.String())
		}
		labels := replicaLabels(replicaLabelName, b)
		group := replicaGroup(labels)
		groupLabels[group] = labels
		if _, ok := m[group]; !ok {
			m[group] = make(map[string][]*metadata.Meta, 0)
		}
		m[group][name] = append(m[group][name], b)
	}
	replicas := make(Replicas, 0)
	for group, v := range m {
		for name, blocks := range v {
			replicas = append(replicas, NewReplica(name, groupLabels[group], blocks))
		}
	}
	return replicas, nil
}

func replicaLabels(replicaLabelName string, b *metadata.Meta) map[string]string {
	labels := make(map[string]string)
	for k, v := range b.Thanos.Labels {
		if k == replicaLabelName {
			continue
		}
		labels[k] = v
	}
	labels[ResolutionLabel] = fmt.Sprint(b.Thanos.Downsample.Resolution)
	return labels
}

func replicaGroup(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for k := range labels {
		names = append(names, k)
	}
	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})
	var b strings.Builder
	b.WriteString("[")
	for i, name := range names {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%s=%s", name, labels[name]))
	}
	b.WriteString("]")
	return b.String()
}
