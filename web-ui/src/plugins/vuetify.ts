import 'vuetify/styles'
import '@mdi/font/css/materialdesignicons.css'
import { createVuetify, type ThemeDefinition } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'

const darkTheme: ThemeDefinition = {
  dark: true,
  colors: {
    background: '#121212',
    surface: '#1E1E1E',
    'surface-bright': '#2D2D2D',
    'surface-light': '#383838',
    'surface-variant': '#424242',
    'on-surface-variant': '#EEEEEE',
    primary: '#7C4DFF',
    'primary-darken-1': '#651FFF',
    secondary: '#00BFA5',
    'secondary-darken-1': '#00897B',
    error: '#FF5252',
    info: '#2196F3',
    success: '#4CAF50',
    warning: '#FFC107',
  },
}

const lightTheme: ThemeDefinition = {
  dark: false,
  colors: {
    background: '#FAFAFA',
    surface: '#FFFFFF',
    'surface-bright': '#FFFFFF',
    'surface-light': '#F5F5F5',
    'surface-variant': '#EEEEEE',
    'on-surface-variant': '#424242',
    primary: '#651FFF',
    'primary-darken-1': '#4615B2',
    secondary: '#00BFA5',
    'secondary-darken-1': '#00897B',
    error: '#D32F2F',
    info: '#1976D2',
    success: '#388E3C',
    warning: '#F57C00',
  },
}

export default createVuetify({
  components,
  directives,
  theme: {
    defaultTheme: 'dark',
    themes: {
      dark: darkTheme,
      light: lightTheme,
    },
  },
  defaults: {
    VCard: {
      elevation: 2,
    },
    VBtn: {
      variant: 'flat',
    },
    VTextField: {
      variant: 'outlined',
      density: 'comfortable',
    },
    VSelect: {
      variant: 'outlined',
      density: 'comfortable',
    },
    VTextarea: {
      variant: 'outlined',
      density: 'comfortable',
    },
  },
})
