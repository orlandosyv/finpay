import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { InfrastructurePage } from './infrastructure-page';

describe('InfrastructurePage', () => {
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [InfrastructurePage],
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('shows live Kafka publication evidence and educational context', () => {
    const fixture = TestBed.createComponent(InfrastructurePage);
    fixture.detectChanges();

    http.expectOne('/api/merchant/infrastructure').flush({
      checkedAt: '2026-09-22T20:00:00Z',
      kafka: {
        status: 'CONNECTED',
        topic: 'finpay.payment-events.v1',
        topicAvailable: true,
        partitions: 3,
        publishingEnabled: true,
        clusterId: 'finpay-cluster',
        totalEvents: 3,
        pendingEvents: 1,
        publishedEvents: 2,
        failedEvents: 0,
        recentEvents: [
          {
            eventId: 'event-1',
            paymentId: 91,
            eventType: 'payment.created',
            status: 'PUBLISHED',
            attempts: 1,
            partition: 2,
            offset: 14,
            createdAt: '2026-09-22T20:00:00Z',
            publishedAt: '2026-09-22T20:00:01Z',
            nextAttemptAt: null,
            lastError: null,
          },
        ],
        hint: 'The publisher is active.',
      },
      webhooks: {
        status: 'ACTIVE',
        activeEndpoints: 1,
        deliveryMode: 'SIGNED_HTTP_WITH_RETRIES',
        hint: 'Payment events can be delivered.',
      },
      pipeline: [
        {
          order: 1,
          name: 'Payment transaction',
          technology: 'SQL Server',
          state: 'ACTIVE',
          hint: 'Source of truth.',
        },
        {
          order: 4,
          name: 'Internal event stream',
          technology: 'Apache Kafka',
          state: 'ACTIVE',
          hint: 'Events are published.',
        },
      ],
    });
    fixture.detectChanges();

    const text = fixture.nativeElement.textContent;
    expect(text).toContain('CONNECTED');
    expect(text).toContain('finpay.payment-events.v1');
    expect(text).toContain('Active');
    expect(text).toContain('payment.created');
    expect(text).toContain('P2 · O14');
    expect(text).toContain('Transactional outbox');
    expect(text).toContain('With Kafka');
    expect(text).toContain('Without webhooks');
  });
});
