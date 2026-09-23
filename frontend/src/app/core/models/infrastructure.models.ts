export interface KafkaInfrastructureStatus {
  status: 'CONNECTED' | 'DISCONNECTED' | 'DISABLED';
  topic: string;
  topicAvailable: boolean;
  partitions: number;
  publishingEnabled: boolean;
  clusterId: string | null;
  hint: string;
}

export interface WebhookInfrastructureStatus {
  status: 'ACTIVE' | 'NOT_CONFIGURED';
  activeEndpoints: number;
  deliveryMode: string;
  hint: string;
}

export interface InfrastructurePipelineStage {
  order: number;
  name: string;
  technology: string;
  state: string;
  hint: string;
}

export interface InfrastructureStatusResponse {
  checkedAt: string;
  kafka: KafkaInfrastructureStatus;
  webhooks: WebhookInfrastructureStatus;
  pipeline: InfrastructurePipelineStage[];
}
