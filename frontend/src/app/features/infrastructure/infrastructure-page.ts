import { DatePipe } from '@angular/common';
import { Component, inject, OnInit, signal } from '@angular/core';
import { finalize } from 'rxjs';
import { InfrastructureApiService } from '../../core/api/infrastructure-api.service';
import { InfrastructureStatusResponse } from '../../core/models/infrastructure.models';
import { requestError } from '../../shared/request-error';

type InfrastructureView = 'live' | 'learn';

@Component({
  selector: 'app-infrastructure-page',
  imports: [DatePipe],
  templateUrl: './infrastructure-page.html',
  styleUrl: './infrastructure-page.scss',
})
export class InfrastructurePage implements OnInit {
  private readonly api = inject(InfrastructureApiService);

  readonly status = signal<InfrastructureStatusResponse | null>(null);
  readonly loading = signal(false);
  readonly error = signal('');
  readonly activeView = signal<InfrastructureView>('live');

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    if (this.loading()) return;
    this.loading.set(true);
    this.error.set('');
    this.api
      .getStatus()
      .pipe(finalize(() => this.loading.set(false)))
      .subscribe({
        next: (status) => this.status.set(status),
        error: (error) => this.error.set(requestError(error)),
      });
  }

  selectView(view: InfrastructureView): void {
    this.activeView.set(view);
  }
}
