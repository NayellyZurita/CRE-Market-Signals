// web/postcss.config.js
/** @type {import('postcss-load-config').Config} */
module.exports = {
  plugins: {
    '@tailwindcss/postcss': {},   // important: object map, not array
  },
};
