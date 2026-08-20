/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        ink: {
          50: '#f6f7f9',
          100: '#eceef2',
          200: '#d5d9e2',
          300: '#b0b8c9',
          400: '#8591aa',
          500: '#66738f',
          600: '#515c76',
          700: '#424a60',
          800: '#394051',
          900: '#141824',
          950: '#0b0e16',
        },
      },
    },
  },
  plugins: [],
}
