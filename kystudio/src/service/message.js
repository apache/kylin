import store from '../store/index'

export function ListenMessage (EventsBus) {
  window.addEventListener('message', (args) => {
    const {data: {action, params}} = args
    if (action) {
      store.commit('COLLECT_MESSAGE_DIRECTIVES', {action, params})
      switch (action) {
        case 'filterModel':
          store.commit('UPDATE_FILTER_MODEL_NAME_CLOUD', params.modelAlias)
          break
        case 'changeLang':
          EventsBus.$emit('changeLang', params)
          break
      }
    }
  })
}
