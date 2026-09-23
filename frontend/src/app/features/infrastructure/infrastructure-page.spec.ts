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

  it('shows live Kafka and webhook status without claiming that publishing is active', () => {
    const fixture = TestBed.createComponent(InfrastructurePage);
    fixture.detectChanges();

    http.expectOne('/api/merchant/infrastructure').flush({
      checkedAt: '2026-09-22T20:00:00Z',
      kafka: {
        status: 'CONNECTED',
        topic: 'finpay.payment-events.v1',
        topicAvailable: true,
        partitions: 3,
        publishingEnabled: false,
        clusterId: 'finpay-cluster',
        hint: 'The broker and topic are ready. No payment events are published until step 2.',
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
          state: 'READY_NOT_PUBLISHING',
          hint: 'Publishing starts in step 2.',
        },
      ],
    });
    fixture.detectChanges();

    const text = fixture.nativeElement.textContent;
    expect(text).toContain('CONNECTED');
    expect(text).toContain('finpay.payment-events.v1');
    expect(text).toContain('Not active yet');
    expect(text).toContain('With Kafka');
    expect(text).toContain('Without webhooks');
  });
});
