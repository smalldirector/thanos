package retention

import (
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type resType uint8

const (
	resRaw resType = iota // raw resolution
	res5m                 // 5m resolution
	res1h                 // 1h resolution
)

func toResType(res int64) resType {
	if res == 0 {
		return resRaw
	}
	if res == 300000 {
		return res5m
	}
	if res == 3600000 {
		return res1h
	}
	panic(fmt.Errorf("invalid resolution %d, supported resolutions [0, 300000, 3600000]", resRaw))
}

type policy struct {
	lset      labels.Labels
	retention time.Duration
}

func NewPolicy(lset labels.Labels, retention time.Duration) *policy {
	return &policy{
		lset:      lset,
		retention: retention,
	}
}

func (p *policy) isDefaultPolicy() bool {
	return p.lset == nil || p.lset.Len() == 0
}

type policyGroup struct {
	active                []*policy
	inactive              []*policy
	applied               []*policy
	isDefaultPolicyActive bool
}

type policyAssigner struct {
	logger  log.Logger
	bkt     objstore.Bucket
	timeout time.Duration
	polices map[resType][]*policy
}

func NewPolicyAssigner(logger log.Logger, bkt objstore.Bucket, timeout time.Duration, polices map[resType][]*policy) *policyAssigner {
	return &policyAssigner{
		logger:  logger,
		bkt:     bkt,
		timeout: timeout,
		polices: polices,
	}
}

// assign active polices for target block
func (pa *policyAssigner) Assign(ctx context.Context, b *metadata.Meta) *policyGroup {
	if b == nil {
		return nil
	}

	appliedPolicies := pa.appliedPolicies(ctx, b.ULID)

	active := make([]*policy, 0, 0)
	inactive := make([]*policy, 0, 0)
	applied := make([]*policy, 0, 0)
	isDefaultPolicyActive := false

	res := b.Thanos.Downsample.Resolution
	candidates := pa.polices[toResType(res)]
	curTimeInMillis := time.Now().UnixNano() / int64(time.Millisecond)
	for _, p := range candidates {
		if pa.isAppliedBefore(appliedPolicies, p) {
			applied = append(applied, p)
		} else if pa.isActive(b, p, curTimeInMillis) {
			active = append(active, p)
			if p.isDefaultPolicy() {
				isDefaultPolicyActive = true
			}
		} else {
			inactive = append(inactive, p)
		}
	}
	return &policyGroup{
		active:                active,
		inactive:              inactive,
		applied:               applied,
		isDefaultPolicyActive: isDefaultPolicyActive,
	}
}

func (pa *policyAssigner) appliedPolicies(ctx context.Context, id ulid.ULID) map[string]*policy {
	res := make(map[string]*policy)
	meta, err := ReadMetaFile(ctx, pa.logger, pa.timeout, pa.bkt, id)
	if err != nil {
		return res
	}
	if meta == nil {
		return res
	}
	for _, mp := range meta.Policies {
		lset, err := promql.ParseMetric(mp.Expr)
		if err != nil {
			continue
		}
		duration, err := time.ParseDuration(mp.Retention)
		if err != nil {
			continue
		}
		res[lset.String()] = NewPolicy(lset, duration)
	}
	return res
}

func (pa *policyAssigner) isActive(b *metadata.Meta, policy *policy, curTimeInMillis int64) bool {
	// run retention function on one block may take some time, for example 30m.
	// for the very beginning series, the policy may be viewed as inactive status at that time,
	// however after 30m, it may meet the retention time then become active policy.
	// in order to avoid keeping stale data in the new block, set one back off time to mitigate such situation.
	// the value depends the retention processing time on each block.
	// TODO make backoff time config configurable later
	// 1 hour back off time
	backoffTimeInMillis := int64(time.Hour) / int64(time.Millisecond)
	retentionTimeInMillis := int64(policy.retention) / int64(time.Millisecond)
	if b.MaxTime+retentionTimeInMillis+backoffTimeInMillis <= curTimeInMillis {
		return true
	}
	return false
}

func (pa *policyAssigner) isAppliedBefore(appliedPolicies map[string]*policy, p *policy) bool {
	key := p.lset.String()
	_, ok := appliedPolicies[key]
	return ok && appliedPolicies[key].retention == p.retention
}

const maxDuration time.Duration = 1<<63 - 1

type PolicyRetentionConfig struct {
	ResRaw model.Duration `yaml:"res-raw"`
	Res5m  model.Duration `yaml:"res-5m"`
	Res1h  model.Duration `yaml:"res-1h"`
}

func (p PolicyRetentionConfig) String() string {
	return fmt.Sprintf("Policy{Raw: %s, 5m: %s, 1h: %s}", p.ResRaw.String(), p.Res5m.String(), p.Res1h.String())
}

type PolicyConfig struct {
	Expr       string                `yaml:"expr"`
	Retentions PolicyRetentionConfig `yaml:"retentions"`
}

func (p PolicyConfig) String() string {
	return fmt.Sprintf("Policy{Expr: %s, Retentions: %s}", p.Expr, p.Retentions.String())
}

type PolicyConfigs struct {
	Configs []PolicyConfig `yaml:"policies"`
}

func (pc *PolicyConfigs) ToPolicies() (map[resType][]*policy, error) {
	res := make(map[resType][]*policy)
	res[resRaw] = make([]*policy, 0)
	res[res5m] = make([]*policy, 0)
	res[res1h] = make([]*policy, 0)

	// use "" or "{}" to indicate default policy
	foundDefaultPolocy := false
	for _, config := range pc.Configs {
		var lset labels.Labels
		var err error
		if len(config.Expr) != 0 {
			lset, err = promql.ParseMetric(config.Expr)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid policy expression definition: %s", config.Expr)
			}
		} else {
			lset = make(labels.Labels, 0)
		}
		if len(lset) == 0 {
			foundDefaultPolocy = true
		}
		if config.Retentions.ResRaw > 0 {
			res[resRaw] = append(res[resRaw], NewPolicy(lset, time.Duration(config.Retentions.ResRaw)))
		} else {
			res[resRaw] = append(res[resRaw], NewPolicy(lset, maxDuration))
		}
		if config.Retentions.Res5m > 0 {
			res[res5m] = append(res[res5m], NewPolicy(lset, time.Duration(config.Retentions.Res5m)))
		} else {
			res[res5m] = append(res[res5m], NewPolicy(lset, maxDuration))
		}
		if config.Retentions.Res1h > 0 {
			res[res1h] = append(res[res1h], NewPolicy(lset, time.Duration(config.Retentions.Res1h)))
		} else {
			res[res1h] = append(res[res1h], NewPolicy(lset, maxDuration))
		}
	}
	if !foundDefaultPolocy {
		lset := make(labels.Labels, 0)
		res[resRaw] = append(res[resRaw], NewPolicy(lset, maxDuration))
		res[res5m] = append(res[res5m], NewPolicy(lset, maxDuration))
		res[res1h] = append(res[res1h], NewPolicy(lset, maxDuration))
	}
	return res, nil
}

func ParsePolicyConfigsFromYaml(confContentYaml []byte) (*PolicyConfigs, error) {
	configs := &PolicyConfigs{}
	if err := yaml.UnmarshalStrict(confContentYaml, configs); err != nil {
		return configs, errors.Wrap(err, "parsing policy config YAML file")
	}
	return configs, nil
}

const AppliedPolicyMetaFile = "applied.policies.json"

type AppliedPolicyMeta struct {
	Version  int              `json:"version"`
	Policies []*AppliedPolicy `json:"policies"`
}

type AppliedPolicy struct {
	Expr      string `json:"expr"`
	Retention string `json:"retention"`
}

func NewAppliedPolicyMeta(pg *policyGroup) *AppliedPolicyMeta {
	res := make([]*AppliedPolicy, 0, len(pg.applied)+len(pg.active))
	for _, p := range pg.applied {
		res = append(res, &AppliedPolicy{
			Expr:      p.lset.String(),
			Retention: p.retention.String(),
		})
	}
	for _, p := range pg.active {
		res = append(res, &AppliedPolicy{
			Expr:      p.lset.String(),
			Retention: p.retention.String(),
		})
	}
	return &AppliedPolicyMeta{
		Version:  1,
		Policies: res,
	}
}

// WriteMetaFile writes the given meta into <dir>/applied.policies.json.
func WriteMetaFile(logger log.Logger, dir string, meta *AppliedPolicyMeta) error {
	// Make any changes to the file appear atomic.
	p := filepath.Join(dir, AppliedPolicyMetaFile)
	tmp := p + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")

	if err := enc.Encode(meta); err != nil {
		runutil.CloseWithLogOnErr(logger, f, "write meta file close")
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return renameFile(logger, tmp, p)
}

func ReadMetaFile(ctx context.Context, logger log.Logger, timeout time.Duration, bkt objstore.Bucket, id ulid.ULID) (*AppliedPolicyMeta, error) {
	workCtx, cancel := TimeoutContext(ctx, timeout)
	defer cancel()
	rc, err := bkt.Get(workCtx, path.Join(id.String(), AppliedPolicyMetaFile))
	if err != nil {
		return nil, errors.Wrapf(err, "meta.json bkt get for %s", id.String())
	}
	defer runutil.CloseWithLogOnErr(logger, rc, "download meta bucket client")
	var m AppliedPolicyMeta

	obj, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, errors.Wrapf(err, "read meta.json for block %s", id.String())
	}

	if err = json.Unmarshal(obj, &m); err != nil {
		return nil, errors.Wrapf(err, "unmarshal meta.json for block %s", id.String())
	}
	if m.Version != 1 {
		return nil, errors.Errorf("unexpected meta file version %d", m.Version)
	}
	return &m, nil
}

func renameFile(logger log.Logger, from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fdatasync(pdir); err != nil {
		runutil.CloseWithLogOnErr(logger, pdir, "rename file dir close")
		return err
	}
	return pdir.Close()
}
