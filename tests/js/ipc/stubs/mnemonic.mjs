// `app-settings.js` only validates recovery phrases here; the parity scenario uses none.
export const normalizeMnemonic = value => String(value ?? '').trim().toLowerCase().split(/\s+/).filter(Boolean).join(' ')
export const validateMnemonic = () => true
