import { mount } from '@vue/test-utils'
import TreeList from '../index.vue'
import { localVue } from '../../../../../test/common/spec_common'
import ElementUI, { Input } from 'kyligence-kylin-ui'

localVue.use(ElementUI)
jest.setTimeout(50000)

const filterData = jest.fn().mockImplementation(() => {
  return new Promise((resolve) => resolve(true))
})
const handleClick = jest.fn()
const handleDbClick = jest.fn()
const _eventMaps = {}
const _removeEventMaps = {}
document.addEventListener = jest.fn((event, callback) => {
  _eventMaps[event] = callback
})
document.removeEventListener = jest.fn((event, callback) => {
  _removeEventMaps[event] = callback
})
const _data = [
  {
    children: [
      {
        children: [
          {
            children: [],
            database: 'SSB',
            datasource: 9,
            dateRange: '',
            id: '082be96c-3d80-4d06-b06c-f4b08a0096dd',
            isCentral: false,
            isMore: true,
            isSelected: false,
            isTopSet: true,
            label: 'CUSTOMER',
            render: jest.fn(),
            child_options: { pageSize: 10, page_offset: 1 },
            childContent: [],
            tags: ['L'],
            type: 'table'
          },
          {
            id: 'isLoading', label: '', type: 'isLoading'
          }
        ],
        datasource: 9,
        id: '9.SSB',
        isDefaultDB: false,
        isHidden: false,
        isLoading: true,
        isMore: true,
        label: 'SSB',
        originTables: [],
        pagination: { pageSize: 10, page_offset: 1 },
        projectName: 'xm_test',
        render: jest.fn(),
        handleClick: handleClick,
        handleDbClick: handleDbClick,
        type: 'database'
      },
      { id: 'isMore', type: 'isMore', text: '' }
    ],
    id: 9,
    label: '数据源：Hive',
    projectName: 'test',
    render: jest.fn(),
    sourceType: 0,
    type: 'datasource'
  }
]
const _event = {
  dataTransfer: {
    text: 'test',
    effectAllowed: '',
    setData: function (tar, value) {
      this[tar] = value
    }
  },
  stopPropagation: jest.fn(),
  target: ''
}
const wrapper = mount(TreeList, {
  localVue,
  propsData: {
    onFilter: filterData,
    data: _data,
    emptyText: '暂无数据',
    filterWhiteListTypes: ['column'],
    isShowFilter: true
  }
})

describe('Component TreeList', () => {
  it('init', async () => {
    expect(wrapper.find('.tree-list').attributes().style).toEqual()
    expect(wrapper.find('.resize-bar').attributes().style).toEqual('display: none;')
    await wrapper.setProps({ isShowResizeBar: true })
    // await wrapper.update()
    expect(wrapper.find('.resize-bar').attributes().style).toEqual('')
    expect(wrapper.vm.treeStyle).toEqual({ width: 0 })
  })
  it('filter data', async () => {
    wrapper.vm.$emit('filter', {target: { value: 'test example', which: 13 }})
    expect(wrapper.vm.filterText).toBe('test example')
    expect(wrapper.vm.isLoading).toBeFalsy()
    // expect(filterData).toBeCalled()

    wrapper.vm.$emit('filter', {target: { value: 'test' }})
    expect(wrapper.vm.filterText).toBe('test')
    expect(wrapper.vm.isLoading).toBeFalsy()
    expect(wrapper.vm.timer).not.toBe(0)

    wrapper.vm.handleInput({ which: 13 })
    expect(filterData).toBeCalled()
  })
  it('filter data fail', async () => {
    const filterFunc = jest.fn().mockRejectedValue()
    await wrapper.setProps({ onFilter: filterFunc })
    // await wrapper.update()
    wrapper.vm.handleFilter()
    expect(wrapper.vm.isLoading).toBeTruthy()
    expect(filterFunc).toBeCalled()
  })
  it('mouse events', () => {
    wrapper.findComponent({ ref: 'resize-bar' }).trigger('mousedown')
    expect(wrapper.vm.isResizing).toBeFalsy()
    expect(wrapper.vm.resizeFrom).toBe(0)
    wrapper.vm.handleResizeStart({which: 1, pageX: 250})
    expect(wrapper.vm.isResizing).toBeTruthy()
    expect(wrapper.vm.resizeFrom).toBe(250)
    _eventMaps.mousemove({pageX: 10})
    expect(wrapper.vm.movement).toBe(0)
    expect(wrapper.vm.treeStyle).toEqual({width: 0})
    expect(wrapper.vm.resizeFrom).toBe(250)
    expect(wrapper.emitted().resize).toBeUndefined()
    _eventMaps.mousemove({pageX: 800})
    expect(wrapper.vm.movement).toBe(0)
    expect(wrapper.vm.treeStyle).toEqual({width: 0})
    expect(wrapper.vm.resizeFrom).toBe(250)
    expect(wrapper.emitted().resize).toBeUndefined()
    _eventMaps.mousemove({pageX: 500})
    expect(wrapper.vm.movement).toBe(250)
    expect(wrapper.vm.treeStyle).toEqual({width: 250})
    expect(wrapper.vm.resizeFrom).toBe(500)
    expect(wrapper.emitted().resize).toEqual([[250]])
    _eventMaps.mouseup()
    expect(wrapper.vm.movement).toBe(0)
    expect(wrapper.vm.resizeFrom).toBe(0)
    expect(wrapper.vm.isResizing).toBeFalsy()
  })
  it('tree event', async () => {
    const vEvent = wrapper.find('.el-tree').vnode.context.$el.__vue__.$parent
    // expect(vEvent.handleNodeFilter.call(wrapper.vm, 'test', _data[0], { isLeaf: false })).toBeTruthy()
    // expect(vEvent.handleNodeFilter.call(wrapper.vm, 'test', _data[0], { isLeaf: true })).toBeFalsy()
    // expect(vEvent.handleNodeFilter.call(wrapper.vm, 'SSB', _data[0].children[0], { isLeaf: true })).toBeTruthy()
    // expect(vEvent.handleNodeFilter.call(wrapper.vm, '', _data[0].children[0], { isLeaf: true })).toBeFalsy()
    _data[0].children.push({id: 'isMore', type: 'isMore', text: ''})
    vEvent.handleNodeClick.call(wrapper.vm, _data[0].children[1], {})
    expect(wrapper.emitted()['load-more'][0].length).toBe(2)
    vEvent.handleNodeClick.call(wrapper.vm, _data[0].children[0], {})
    expect(wrapper.emitted().click[0].length).toBe(2)
    _data[0].children[0].children[0].children.unshift({
      text: 'columns',
      label: 'column1',
      parent: (function () {
        return _data[0].children[0].children[0]
      })(),
      type: 'isMore'
    })
    vEvent.handleNodeClick.call(wrapper.vm, _data[0].children[0].children[0].children[0], {})
    expect(_data[0].children[0].children[0]['child_options'].page_offset).toBe(2)
    expect(_data[0].children[0].children[0].isMore).toBeFalsy()
    vEvent.patchNodeLoading.call(null, _data[0].children[0])
    expect(_data[0].children[0].children.filter(it => it.type === 'isLoading').length).toBeTruthy()
    // _data[0].children[0].children.push({ id: 'isLoading', label: '', type: 'isLoading', parent: {} })
    vEvent.patchNodeLoading.call(null, _data[0].children[0])
    expect(_data[0].children[0].children.filter(it => it.type === 'isLoading').length).toBe(1)
    _data[0].children[0].isLoading = false
    vEvent.patchNodeLoading.call(null, _data[0].children[0])
    expect(_data[0].children[0].children.length).toBe(2)
    vEvent.patchNodeMore.call(null, _data[0].children[0])
    expect(_data[0].children[0].children.filter(it => it.id === 'isMore').length).toBeTruthy()
    _data[0].children[0].isMore = false
    vEvent.patchNodeMore.call(null, _data[0].children[0])
    expect(_data[0].children[0].children.length).toBe(1)
    vEvent.handleDragstart.call(wrapper.vm, _event, _data[0].children[0])
    expect(_event.dataTransfer.effectAllowed).toBe('move')
    expect(_event.stopPropagation).toBeCalled()
    expect(wrapper.emitted().drag.length).toBe(1)
    expect(_event.dataTransfer.text).toBe('')
    vEvent.handleNodeExpand.call(wrapper.vm)
    expect(wrapper.emitted()['node-expand'][0].length).toBe(2)
    vEvent.handleMouseDown(_event)
    expect(_event.stopPropagation).toBeCalled()

    expect(wrapper.vm.getTreeItemStyle(_data[0].children[1], {level: 2})).toEqual({'paddingLeft': 'calc(42px)', 'transform': 'translateX(-42px)', 'width': 'calc(100% + 18px)'})
    // const _node = { level: 2, label: 'database' }
    // const _h = jest.fn()
    // vEvent.renderNode.call(wrapper.vm, _h, {node: _node, data: {..._data[0].children[0].children[0], 'handleClick': handleClick, 'handleDbClick': handleDbClick}})
    // expect(_h).toBeCalled()
    wrapper.find('.tree-item').trigger('mousedown')
    wrapper.find('.tree-item').trigger('dragstart')
    wrapper.findAll('.tree-item').trigger('click')
    expect(handleClick).toHaveBeenCalledTimes(1)
    wrapper.findAll('.tree-item').trigger('dbClick')
    expect(handleDbClick).toBeCalled()
  })
  it('unMount component', () => {
    wrapper.destroy()
    expect(Object.keys(_removeEventMaps).length).toBe(2)
  })
  it('no data', () => {
    const wrapper1 = mount(TreeList, {
      localVue,
      propsData: {
        onFilter: filterData,
        // data: _data,
        emptyText: '暂无数据'
        // filterWhiteListTypes: ['column']
      }
    })
    expect(wrapper1.find('.el-tree').find('.el-tree__empty-text').text()).toBe('暂无数据')
  })
})
