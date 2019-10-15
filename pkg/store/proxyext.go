package store

import (
	"context"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/thanos"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/util"
)

type matchers func(r util.SeriesRequest, s map[string]thanos.Pair) ([]interface{}, error)

func getMatchedStores(cxt context.Context, stores []Client, matchers matchers, r *storepb.SeriesRequest, log log.Logger) ([]interface{}, error) {

	customStore, cxt := tracing.StartSpan(cxt, "sherlockio_store_matches")
	defer customStore.Finish()

	ipMap := make(map[string]thanos.Pair)
	for _, sc := range stores {
		addr := sc.Addr()
		ip := thanos.GetIPAddress(addr)
		if ip == "" {
			level.Warn(log).Log("select_stores", fmt.Sprintf("cannot get ip address: %s\n", addr))
		} else {
			ipMap[ip] = thanos.Pair{addr, sc}
		}
	}

	var err error
	var matched []interface{}
	if matchers != nil && len(stores) > 0 {
		ipAdd := make([]string, len(stores))
		for _, st := range stores {
			ipAdd = append(ipAdd, st.Addr())
		}

		mapper := make(map[string]util.Matchers)
		for _, m := range r.Matchers {
			if m.Type == storepb.LabelMatcher_EQ {
				mm := util.Matchers{
					1,
					m.Value,
				}
				mapper[m.Name] = mm
			}
		}

		p := util.SeriesRequest{
			r.MinTime,
			r.MaxTime,
			mapper,
		}
		matched, err = matchers(p, ipMap)
		if err != nil {
			return nil, err
		}

	}
	customStore.LogKV("stores_matched", fmt.Sprintf("[%d]", len(matched)))
	return matched, nil

}