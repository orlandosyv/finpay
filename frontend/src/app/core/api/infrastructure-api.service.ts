import { HttpClient } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { InfrastructureStatusResponse } from '../models/infrastructure.models';

@Injectable({ providedIn: 'root' })
export class InfrastructureApiService {
  private readonly http = inject(HttpClient);

  getStatus(): Observable<InfrastructureStatusResponse> {
    return this.http.get<InfrastructureStatusResponse>('/api/merchant/infrastructure');
  }
}
