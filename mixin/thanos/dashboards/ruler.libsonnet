local g = import '../thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  ruler+:: {
    jobPrefix: error 'must provide job prefix for Thanos Ruler dashboard',
    selector: error 'must provide selector for Thanos Ruler dashboard',
    title: error 'must provide title for Thanos Ruler dashboard',
  },
  grafanaDashboards+:: {
    'ruler.json':
      g.dashboard(thanos.ruler.title)
      .addRow(
        g.row('Alert Sent')
        .addPanel(
          g.panel('Dropped Rate', 'Shows rate of dropped alerts.') +
          g.queryPanel(
            'sum(rate(thanos_alert_sender_alerts_dropped_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, alertmanager)',
            '{{job}} {{alertmanager}}'
          )
        )
        .addPanel(
          g.panel('Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
          g.queryPanel(
            'sum(rate(thanos_alert_sender_alerts_sent_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, alertmanager)',
            '{{job}} {{alertmanager}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_sender_errors_total{namespace="$namespace",job=~"$job"}',
            'thanos_alert_sender_alerts_sent_total{namespace="$namespace",job=~"$job"}',
          )
        )
        .addPanel(
          g.panel('Sent Duration', 'Shows how long has it taken to send alerts to alert manager.') +
          g.latencyPanel('thanos_alert_sender_latency_seconds', 'namespace="$namespace",job=~"$job"'),
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles.') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
      )
      .addRow(
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests.') +
          g.grpcQpsPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrDetailsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles.') +
          g.grpcLatencyPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        ) +
        g.collapse
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
      )
      .addRow(
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests.') +
          g.grpcQpsPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrDetailsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles') +
          g.grpcLatencyPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        ) +
        g.collapse
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', thanos.dashboard.namespaceQuery) +
      g.template('job', 'up', 'namespace="$namespace",%(selector)s' % thanos.ruler, true, '%(jobPrefix)s.*' % thanos.ruler) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(jobPrefix)s.*"' % thanos.ruler, true, '.*'),

    __overviewRows__+:: [
      g.row('Rule')
      .addPanel(
        g.panel('Alert Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
        g.queryPanel(
          'sum(rate(thanos_alert_sender_alerts_sent_total{namespace="$namespace",%(selector)s}[$interval])) by (job, alertmanager)' % thanos.ruler,
          '{{job}} {{alertmanager}}'
        ) +
        g.addDashboardLink(thanos.ruler.title) +
        g.stack
      )
      .addPanel(
        g.panel('Alert Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
        g.qpsErrTotalPanel(
          'thanos_alert_sender_errors_total{namespace="$namespace",%(selector)s}' % thanos.ruler,
          'thanos_alert_sender_alerts_sent_total{namespace="$namespace",%(selector)s}' % thanos.ruler,
        ) +
        g.addDashboardLink(thanos.ruler.title)
      )
      .addPanel(
        g.sloLatency(
          'Alert Sent Duration',
          'Shows how long has it taken to send alerts to alert manager.',
          'thanos_alert_sender_latency_seconds_bucket{namespace="$namespace",%(selector)s}' % thanos.ruler,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.ruler.title)
      ) +
      g.collapse,
    ],
  },
}
