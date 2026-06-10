package metrics

import (
	"context"
	"encoding/json"

	luxlog "github.com/luxfi/log"
	metric "github.com/luxfi/metric"
	zap "github.com/luxfi/zap"
)

// MsgMetricBatch is the canonical ZAP MsgType for metric batches — it matches
// luxfi/metric.MsgMetricBatch and o11y/pkg/zapmetricreceiver so any luxfi/metric
// ZAP exporter ingests here unchanged.
const MsgMetricBatch uint16 = 2

// zapReceiver hosts a luxfi/zap node that accepts MsgMetricBatch messages and
// ingests them into the addressed tenant's metric store.
type zapReceiver struct{ node *zap.Node }

// startZAPReceiver binds a ZAP node on port that decodes a luxfi/metric.MetricBatch
// (JSON at message field 0, the o11y wire convention) from each MsgMetricBatch
// message and writes it into reg.For(org).Metrics — org is the batch's
// Resource["org"], else the deployment brand. port<=0 disables it (HTTP
// /v1/metrics/batch stays the ingest). The ZAP transport avoids the HTTP hop and
// is the push path for in-cluster luxfi/metric exporters.
func startZAPReceiver(port int, nodeID string, log luxlog.Logger) (*zapReceiver, error) {
	if port <= 0 {
		return nil, nil
	}
	node := zap.NewNode(zap.NodeConfig{NodeID: nodeID, Port: port, NoDiscovery: true})
	node.Handle(MsgMetricBatch, func(ctx context.Context, peerID string, msg *zap.Message) (*zap.Message, error) {
		var b metric.MetricBatch
		if err := json.Unmarshal(msg.Root().Bytes(0), &b); err != nil {
			log.Warn("zap metric batch decode", "peer", peerID, "err", err)
			return nil, nil
		}
		org := b.Resource["org"]
		if org == "" {
			org = brand
		}
		reg.For(org).Metrics.IngestBatch(&b)
		return nil, nil
	})
	if err := node.Start(); err != nil {
		return nil, err
	}
	log.Info("zap metric receiver listening", "port", port, "nodeID", nodeID, "msgType", MsgMetricBatch)
	return &zapReceiver{node: node}, nil
}
