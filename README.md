# jrshop-catalog-

## Firebase security and deployment artifacts

This repository now includes first-phase Firebase security files:

- `firestore.rules` - role-based Firestore access policy
- `storage.rules` - media upload/download policy for authenticated users
- `firebase.json` - points Firebase CLI to these rules
- `FIREBASE_SECURITY.md` - deployment and role model guide

Apply rules with Firebase CLI:

```bash
firebase deploy --only firestore:rules,storage
```