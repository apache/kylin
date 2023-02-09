import { fromArrToObj } from '../../../util'

export const fieldVisiableMaps = {
  'new': ['name', 'type', 'description'],
  'edit': ['name', 'description', 'configuration']
}

export const titleMaps = {
  'new': 'addProject',
  'edit': 'project'
}

export const disabledProperties = [
  'kylin.source.default'
]

export function getSubmitData (that) {
  const { editType, form } = that

  switch (editType) {
    case 'new':
      return JSON.stringify({
        name: form.name,
        description: form.description,
        override_kylin_properties: fromArrToObj(form.properties)
      })
    case 'edit':
      const submitForm = JSON.parse(JSON.stringify(form))

      submitForm.override_kylin_properties = fromArrToObj(submitForm.properties)

      delete submitForm.properties

      return {
        name: submitForm.name,
        desc: JSON.stringify(submitForm)
      }
  }
}
