export const download = {
  post (url, data) {
    const $form = document.createElement('form')
    $form.style.display = 'none'
    $form.method = 'POST'
    $form.action = url
    $form.target = '_blank'
    $form.enctype = 'application/x-www-form-urlencoded'

    for (const [key, value] of Object.entries(data)) {
      if (value instanceof Array) {
        value.forEach((item, index) => {
          const $input = document.createElement('input')
          $input.name = `${key}[${index}]`
          $input.value = item
          $form.appendChild($input)
        })
      } else if (typeof value === 'object') {
        const $input = document.createElement('input')
        $input.name = key
        $input.value = JSON.stringify($input.value)
        $form.appendChild($input)
      } else {
        const $input = document.createElement('input')
        $input.name = key
        $input.value = value
        $form.appendChild($input)
      }
    }

    document.body.appendChild($form)
    $form.submit()

    setTimeout(() => {
      document.body.removeChild($form)
    })
  }
}
