# Firebase Security and Role Model

This project uses Firebase-first runtime with Firestore, Storage, and optional Firebase Auth mapping.

## Collections

- `orders`
- `app_data`
- `media`
- `catalog_orders`
- `audit_logs`
- `user_roles`

## Auth + Role sources

The app supports role resolution from:

1. Firebase custom claim: `role` (`Admin`, `Staff`, `Reseller`)
2. Firestore fallback: `user_roles/{uid}` or `user_roles/{email-lowercase}`

When users sign in via app login, the app synchronizes role records into:

- `user_roles/{uid}`
- `user_roles/{email-lowercase}`

## Rules behavior

- Admins/Staff can read/write all core app collections.
- Resellers can read everything needed for app functionality, but write restrictions are applied:
  - `orders`: resellers can only write orders where `resellerId == auth.uid` or `resellerEmail == auth.token.email`
  - `catalog_orders`: reseller scoped similarly
  - `app_data`: protected subkeys for `jb_feed`, `jb_users`, `jb_suppliers`, `jb_settings`, `jb_templates`, `jb_notif` are staff/admin only.
  - `audit_logs`: append-only by signed users; no client-side update/delete.

## Storage rules behavior

- `media/**`:
  - read: signed users
  - write: `Admin` / `Staff` / `Reseller` roles
- Other paths: denied.

## Deployment

Deploy rules with Firebase CLI:

```bash
firebase deploy --only firestore:rules,storage
```

