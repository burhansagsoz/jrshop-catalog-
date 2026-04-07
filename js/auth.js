// ════════════════════════════════════════════════════
// JRSHOP Auth Modülü — Kullanıcı oturumu yönetimi
// ════════════════════════════════════════════════════

const Auth = (() => {

  const KEYS = {
    session:  'jb_me',
    remember: 'jb_remember',
    email:    'jb_remember_email',
    password: 'jb_remember_pw',
  };

  return {

    // ── Oturum ──
    getUser() {
      try {
        const raw = localStorage.getItem(KEYS.session);
        return raw ? JSON.parse(raw) : null;
      } catch { return null; }
    },

    setUser(user) {
      localStorage.setItem(KEYS.session, JSON.stringify(user));
    },

    logout() {
      localStorage.removeItem(KEYS.session);
    },

    isLoggedIn() {
      return !!this.getUser();
    },

    // ── Rol kontrolü ──
    isAdmin() {
      const u = this.getUser();
      return u?.role === 'Admin';
    },

    isReseller() {
      const u = this.getUser();
      return u?.role === 'Reseller';
    },

    isStaff() {
      const u = this.getUser();
      return u?.role === 'Staff';
    },

    // ── Beni hatırla ──
    saveRemember(email, password) {
      localStorage.setItem(KEYS.remember, '1');
      localStorage.setItem(KEYS.email, email);
      localStorage.setItem(KEYS.password, password);
    },

    clearRemember() {
      localStorage.removeItem(KEYS.remember);
      localStorage.removeItem(KEYS.email);
      localStorage.removeItem(KEYS.password);
    },

    getRemembered() {
      if (localStorage.getItem(KEYS.remember) !== '1') return null;
      return {
        email:    localStorage.getItem(KEYS.email)    || '',
        password: localStorage.getItem(KEYS.password) || ''
      };
    },

    isRemembered() {
      return localStorage.getItem(KEYS.remember) === '1';
    }
  };

})();
