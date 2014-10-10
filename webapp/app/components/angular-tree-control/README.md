Angular Tree Control [![Build Status](https://travis-ci.org/wix/angular-tree-control.png)](https://travis-ci.org/wix/angular-tree-control) [![Coverage Status](https://coveralls.io/repos/wix/angular-tree-control/badge.png)](https://coveralls.io/r/wix/angular-tree-control)
================

Pure [AngularJS](http://www.angularjs.org) based tree control component.

[![ScreenShot](https://raw.github.com/wix/angular-tree-control/master/images/sample.png)](http://jsfiddle.net/8ApLX/5/)

Running sample on [jsFiddle](http://jsfiddle.net/8ApLX/5/)

## Why yet another tree control

We have tried a number of tree controls built for angular and experience a of issues with each. As a result we decided
to build a new tree control with the following design guidelines

- Isolated scope - the tree control should not pollute the scope it is rendered at
- Does not change the tree data - some tree implementations mark on the tree data the selection and expansion of nodes
- Allows customization of the tree node label using the angular way - as an angular template
- Supports large trees with minimal overhead
- Reacts to changes in the tree data, updating the tree as required
- Supports css styling, with three built in styles

## Installation

**Bower**: `bower install angular-tree-control`

The tree control can be used as a Dom element or as an attribute.

Copy the script and css into your project and add a script and link tag to your page.

```html
<script type="text/javascript" src="/angular-tree-control.js"></script>
<!-- link for CSS when using the tree as a Dom element -->
<link rel="stylesheet" type="text/css" href="css/tree-control.css">
<!-- link for CSS when using the tree as an attribute -->
<link rel="stylesheet" type="text/css" href="css/tree-control-attribute.css">
```

Add a dependency to your application module.

```javascript
angular.module('myApp', ['treeControl']);
```

Add tree elements to your Angular template

```html
<!-- as a Dom element -->
<treecontrol class="tree-classic"
   tree-model="dataForTheTree"
   options="treeOptions"
   on-selection="showSelected(node)"
   selected-node="node1">
   employee: {{node.name}} age {{node.age}}
</treecontrol>
<!-- as an attribute -->
<div treecontrol class="tree-classic"
   tree-model="dataForTheTree"
   options="treeOptions"
   on-selection="showSelected(node)"
   selected-node="node1">
   employee: {{node.name}} age {{node.age}}
</div>
```

and add the data for the tree

```javascript
$scope.treeOptions = {
    nodeChildren: "children",
    dirSelectable: true,
    injectClasses: {
        ul: "a1",
        li: "a2",
        liSelected: "a7",
        iExpanded: "a3",
        iCollapsed: "a4",
        iLeaf: "a5",
        label: "a6",
        labelSelected: "a8"
    }
}
$scope.dataForTheTree =
[
	{ "name" : "Joe", "age" : "21", "children" : [
		{ "name" : "Smith", "age" : "42", "children" : [] },
		{ "name" : "Gary", "age" : "21", "children" : [
			{ "name" : "Jenifer", "age" : "23", "children" : [
				{ "name" : "Dani", "age" : "32", "children" : [] },
				{ "name" : "Max", "age" : "34", "children" : [] }
			]}
		]}
	]},
	{ "name" : "Albert", "age" : "33", "children" : [] },
	{ "name" : "Ron", "age" : "29", "children" : [] }
];
```


## Usage

Attributes of angular treecontrol

- `treecontrol` : the treeview element.
- element content : the template to evaluate against each node (and the parent scope of the tree) for the node label.
- `tree-model` : the tree data on the `$scope`. This can be an array of nodes or a single node.
- `options` : different options to customize the tree control.
  - `nodeChildren` : the name of the property of each node that holds the node children. Defaults to 'children'.
  - `dirSelection` : are directories (nodes with children) selectable? If not, clicking on the dir label will expand and contact the dir. Defaults to `true`.
  - `equality` : the function used to determine equality between old nodes and new ones when checking whether a replacement node should be expanded and/or marked as selected. Defaults to a function which uses `angular.equals()` on everything except the property indicated in `nodeChildren`.
  - `injectClasses` : allows to inject additional CSS classes into the tree DOM
    - `ul` : inject classes into the ul elements
    - `li` : inject classes into the li elements
    - `liSelected` : inject classes into the li elements only when the node is selected
    - `iExpanded` : inhect classes into the 'i' element for the expanded nodes
    - `iCollapsed` : inhect classes into the 'i' element for the collapsed nodes
    - `iLeaf` : inhect classes into the 'i' element for leaf nodes
    - `label` : inhect classes into the div element around the label
    - `labelSelected` : inhect classes into the div element around the label only when the node is selected
- `on-selection` : function to call on the current `$scope` on node selection.
- `selected-node` : parameter on the `$scope` to update with the current selection.

### The tree labels

The Angular Tree control uses a similar paradigm to ng-repeat in that it allows using the current node as well as values from
the parent scope. The current node is injected into the scope used to render the label as the ```node``` member (unlike ng-repeat, we
do not allow to name the current node item in the transcluded scope).

In order to render a template that takes a value ```X``` from the parent scope of the tree and value ```Y``` from the current node, use the following template
```{{X}} {{node.Y}}```


## Styling

The angular-tree-control renders to the following DOM structure
```html
<treecontrol class="tree-classic">
  <ul>
    <li class="tree-expanded">
      <i class="tree-branch-head"></i>
      <i class="tree-leaf-head"></i>
      <div class="tree-label">
         ... label - expanded angular template is in the treecontrol element ...
      </div>
      <treeitem>
        <ul>
          <li class="tree-leaf">
            <i class="tree-branch-head"></i>
            <i class="tree-leaf-head"></i>
            <div class="tree-label tree-selected">
              ... label - expanded angular template is in the treecontrol element ...
            </div>
          </li>
          <li class="tree-leaf">
            <i class="tree-branch-head"></i>
            <i class="tree-leaf-head"></i>
            <div class="tree-label">
              ... label - expanded angular template is in the treecontrol element ...
            </div>
          </li>
        </ul>
      </treeitem>
    </li>
  </ul>
</treecontrol>
```

The following CSS classes are used in the built-in styles for the tree-control.
Additional classes can be added using the options.injectClasses member (see above)

- tree-expanded, tree-collapsed, tree-leaf - are placed on the 'ul' element
- tree-branch-head, tree-leaf-head - are placed on the 'i' elements. We use those classes to place the icons for the tree
- tree-selected - placed on the div around the label


## Reference

This tree control is based in part on the angular.treeview component
* angular.treeview: http://ngmodules.org/modules/angular.treeview

## License

The MIT License.

See [LICENSE](https://github.com/wix/angular-tree-control/blob/master/LICENSE)
