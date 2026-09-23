export interface KafkaInfrastructureStatus {
  status: 'CONNECTED' | 'DISCONNECTED' | 'DISABLED';
  topic: string;
  topicAvailable: boolean;
  partitions: number;
  publishingEnabled: boolean;
  clusterId: string | null;
  totalEvents: number;
  pendingEvents: number;
  publishedEvents: number;
  failedEvents: number;
  recentEvents: KafkaEventStatus[];
  hint: string;
}

export interface KafkaEventStatus {
  eventId: string;
  paymentId: number;
  eventType: string;
  status: 'PENDING' | 'PUBLISHED' | 'FAILED';
  attempts: number;
  partition: number | null;
  offset: number | null;
  createdAt: string;
  publishedAt: string | null;
  nextAttemptAt: string | null;
  lastError: string | null;
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
