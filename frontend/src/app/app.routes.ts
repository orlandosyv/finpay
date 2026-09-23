import { Routes } from '@angular/router';
import { authGuard } from './core/auth/auth.guard';
import { adminGuard } from './core/auth/admin.guard';
import { guestGuard } from './core/auth/guest.guard';

export const routes: Routes = [
  {
    path: 'register',
    loadComponent: () =>
      import('./features/auth/register-page/register-page').then((m) => m.RegisterPage),
    canActivate: [guestGuard],
    title: 'Register | FinPay Console',
  },
  {
    path: 'login',
    loadComponent: () =>
      import('./features/auth/login-page/login-page').then((component) => component.LoginPage),
    canActivate: [guestGuard],
    title: 'Sign in | FinPay Console',
  },
  {
    path: 'app',
    loadComponent: () =>
      import('./layout/app-shell/app-shell').then((component) => component.AppShell),
    canActivate: [authGuard],
    canActivateChild: [authGuard],
    children: [
      {
        path: 'overview',
        loadComponent: () =>
          import('./features/session/session-page/session-page').then(
            (component) => component.SessionPage,
          ),
        title: 'Overview | FinPay Console',
      },
      {
        path: 'payments',
        loadComponent: () =>
          import('./features/payments/payments-page').then((m) => m.PaymentsPage),
        data: { mode: 'list' },
        title: 'Payments | FinPay Console',
      },
      {
        path: 'payments/new',
        loadComponent: () =>
          import('./features/payments/payments-page').then((m) => m.PaymentsPage),
        data: { mode: 'new' },
        title: 'Create payment | FinPay Console',
      },
      {
        path: 'payments/:id',
        loadComponent: () =>
          import('./features/payments/payments-page').then((m) => m.PaymentsPage),
        data: { mode: 'detail' },
        title: 'Payment details | FinPay Console',
      },
      {
        path: 'team',
        loadComponent: () =>
          import('./features/team/team-page').then((component) => component.TeamPage),
        canActivate: [adminGuard],
        title: 'Team | FinPay Console',
      },
      {
        path: 'webhooks',
        loadComponent: () =>
          import('./features/webhooks/webhook-endpoints-page').then(
            (component) => component.WebhookEndpointsPage,
          ),
        canActivate: [adminGuard],
        title: 'Webhooks | FinPay Console',
      },
      {
        path: 'webhook-events/:eventId',
        loadComponent: () =>
          import('./features/webhooks/webhook-dashboard-page').then(
            (component) => component.WebhookDashboardPage,
          ),
        canActivate: [adminGuard],
        data: { mode: 'detail' },
        title: 'Webhook event | FinPay Console',
      },
      {
        path: 'webhook-events',
        loadComponent: () =>
          import('./features/webhooks/webhook-dashboard-page').then(
            (component) => component.WebhookDashboardPage,
          ),
        canActivate: [adminGuard],
        data: { mode: 'list' },
        title: 'Webhook deliveries | FinPay Console',
      },
      {
        path: 'infrastructure',
        loadComponent: () =>
          import('./features/infrastructure/infrastructure-page').then(
            (component) => component.InfrastructurePage,
          ),
        canActivate: [adminGuard],
        title: 'Infrastructure lab | FinPay Console',
      },
      {
        path: '',
        pathMatch: 'full',
        redirectTo: 'overview',
      },
      {
        path: '**',
        redirectTo: 'overview',
      },
    ],
  },
  {
    path: 'system/health',
    loadComponent: () =>
      import('./features/system/health-page/health-page').then((component) => component.HealthPage),
    title: 'API status | FinPay Console',
  },
  {
    path: '',
    pathMatch: 'full',
    redirectTo: 'login',
  },
  {
    path: '**',
    redirectTo: 'login',
  },
];
