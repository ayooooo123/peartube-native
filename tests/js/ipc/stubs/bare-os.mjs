import { clone, interfaces } from '../state.mjs'

export default {
  networkInterfaces: () => clone(interfaces.current)
}
