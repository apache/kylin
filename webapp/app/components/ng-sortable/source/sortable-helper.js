/*jshint indent: 2 */
/*global angular: false */

(function () {
  'use strict';

  var mainModule = angular.module('ui.sortable');

  /**
   * Helper factory for sortable.
   */
  mainModule.factory('$helper', ['$document', '$window',
    function ($document, $window) {
      return {

        /**
         * Get the height of an element.
         *
         * @param {Object} element Angular element.
         * @returns {String} Height
         */
        height: function (element) {
          return element.prop('offsetHeight');
        },

        /**
         * Get the width of an element.
         *
         * @param {Object} element Angular element.
         * @returns {String} Width
         */
        width: function (element) {
          return element.prop('offsetWidth');
        },

        /**
         * Get the offset values of an element.
         *
         * @param {Object} element Angular element.
         * @param {Object} [scrollableContainer] Scrollable container object for calculating relative top & left (optional, defaults to Document)
         * @returns {Object} Object with properties width, height, top and left
         */
        offset: function (element, scrollableContainer) {
          var boundingClientRect = element[0].getBoundingClientRect();
          if (!scrollableContainer) { scrollableContainer = $document[0].documentElement; }

          return {
            width: boundingClientRect.width || element.prop('offsetWidth'),
            height: boundingClientRect.height || element.prop('offsetHeight'),
            top: boundingClientRect.top + ($window.pageYOffset || scrollableContainer.scrollTop - scrollableContainer.offsetTop),
            left: boundingClientRect.left + ($window.pageXOffset || scrollableContainer.scrollLeft - scrollableContainer.offsetLeft)
          };
        },

        /**
         * get the event object for touch.
         *
         * @param  {Object} event the touch event
         * @return {Object} the touch event object.
         */
        eventObj: function (event) {
          var obj = event;
          if (event.targetTouches !== undefined) {
            obj = event.targetTouches.item(0);
          } else if (event.originalEvent !== undefined && event.originalEvent.targetTouches !== undefined) {
            obj = event.originalEvent.targetTouches.item(0);
          }
          return obj;
        },

        /**
         * Checks whether the touch is valid and multiple.
         *
         * @param event the event object.
         * @returns {boolean} true if touch is multiple.
         */
        isTouchInvalid: function (event) {

          var touchInvalid = false;
          if (event.touches !== undefined && event.touches.length > 1) {
            touchInvalid = true;
          } else if (event.originalEvent !== undefined &&
            event.originalEvent.touches !== undefined && event.originalEvent.touches.length > 1) {
            touchInvalid = true;
          }
          return touchInvalid;
        },

        /**
         * Get the start position of the target element according to the provided event properties.
         *
         * @param {Object} event Event
         * @param {Object} target Target element
         * @param {Object} [scrollableContainer] (optional) Scrollable container object
         * @returns {Object} Object with properties offsetX, offsetY.
         */
        positionStarted: function (event, target, scrollableContainer) {
          var pos = {};
          pos.offsetX = event.pageX - this.offset(target, scrollableContainer).left;
          pos.offsetY = event.pageY - this.offset(target, scrollableContainer).top;
          pos.startX = pos.lastX = event.pageX;
          pos.startY = pos.lastY = event.pageY;
          pos.nowX = pos.nowY = pos.distX = pos.distY = pos.dirAx = 0;
          pos.dirX = pos.dirY = pos.lastDirX = pos.lastDirY = pos.distAxX = pos.distAxY = 0;
          return pos;
        },

        /**
         * Calculates the event position and sets the direction
         * properties.
         *
         * @param pos the current position of the element.
         * @param event the move event.
         */
        calculatePosition: function (pos, event) {
          // mouse position last events
          pos.lastX = pos.nowX;
          pos.lastY = pos.nowY;

          // mouse position this events
          pos.nowX = event.pageX;
          pos.nowY = event.pageY;

          // distance mouse moved between events
          pos.distX = pos.nowX - pos.lastX;
          pos.distY = pos.nowY - pos.lastY;

          // direction mouse was moving
          pos.lastDirX = pos.dirX;
          pos.lastDirY = pos.dirY;

          // direction mouse is now moving (on both axis)
          pos.dirX = pos.distX === 0 ? 0 : pos.distX > 0 ? 1 : -1;
          pos.dirY = pos.distY === 0 ? 0 : pos.distY > 0 ? 1 : -1;

          // axis mouse is now moving on
          var newAx = Math.abs(pos.distX) > Math.abs(pos.distY) ? 1 : 0;

          // calc distance moved on this axis (and direction)
          if (pos.dirAx !== newAx) {
            pos.distAxX = 0;
            pos.distAxY = 0;
          } else {
            pos.distAxX += Math.abs(pos.distX);
            if (pos.dirX !== 0 && pos.dirX !== pos.lastDirX) {
              pos.distAxX = 0;
            }

            pos.distAxY += Math.abs(pos.distY);
            if (pos.dirY !== 0 && pos.dirY !== pos.lastDirY) {
              pos.distAxY = 0;
            }
          }
          pos.dirAx = newAx;
        },

        /**
         * Move the position by applying style.
         *
         * @param event the event object
         * @param element - the dom element
         * @param pos - current position
         * @param container - the bounding container.
         * @param containerPositioning - absolute or relative positioning.
         * @param {Object} [scrollableContainer] (optional) Scrollable container object
         */
        movePosition: function (event, element, pos, container, containerPositioning, scrollableContainer) {
          var bounds;
          var useRelative = (containerPositioning === 'relative');

          element.x = event.pageX - pos.offsetX;
          element.y = event.pageY - pos.offsetY;

          if (container) {
            bounds = this.offset(container, scrollableContainer);

            if (useRelative) {
              // reduce positioning by bounds
              element.x -= bounds.left;
              element.y -= bounds.top;

              // reset bounds
              bounds.left = 0;
              bounds.top = 0;
            }

            if (element.x < bounds.left) {
              element.x = bounds.left;
            } else if (element.x >= bounds.width + bounds.left - this.offset(element).width) {
              element.x = bounds.width + bounds.left - this.offset(element).width;
            }
            if (element.y < bounds.top) {
              element.y = bounds.top;
            } else if (element.y >= bounds.height + bounds.top - this.offset(element).height) {
              element.y = bounds.height + bounds.top - this.offset(element).height;
            }
          }

          element.css({
            'left': element.x + 'px',
            'top': element.y + 'px'
          });

          this.calculatePosition(pos, event);
        },

        /**
         * The drag item info and functions.
         * retains the item info before and after move.
         * holds source item and target scope.
         *
         * @param item - the drag item
         * @returns {{index: *, parent: *, source: *,
                 *          sourceInfo: {index: *, itemScope: (*|.dragItem.sourceInfo.itemScope|$scope.itemScope|itemScope), sortableScope: *},
                 *         moveTo: moveTo, isSameParent: isSameParent, isOrderChanged: isOrderChanged, eventArgs: eventArgs, apply: apply}}
         */
        dragItem: function (item) {

          return {
            index: item.index(),
            parent: item.sortableScope,
            source: item,
            sourceInfo: {
              index: item.index(),
              itemScope: item.itemScope,
              sortableScope: item.sortableScope
            },
            moveTo: function (parent, index) { // Move the item to a new position
              this.parent = parent;
              //If source Item is in the same Parent.
              if (this.isSameParent() && this.source.index() < index) { // and target after
                index = index - 1;
              }
              this.index = index;
            },
            isSameParent: function () {
              return this.parent.element === this.sourceInfo.sortableScope.element;
            },
            isOrderChanged: function () {
              return this.index !== this.sourceInfo.index;
            },
            eventArgs: function () {
              return {
                source: this.sourceInfo,
                dest: {
                  index: this.index,
                  sortableScope: this.parent
                }
              };
            },
            apply: function () {
              this.sourceInfo.sortableScope.removeItem(this.sourceInfo.index); // Remove from source.
              this.parent.insertItem(this.index, this.source.modelValue); // Insert in to destination.
            }
          };
        },

        /**
         * Check the drag is not allowed for the element.
         *
         * @param element - the element to check
         * @returns {boolean} - true if drag is not allowed.
         */
        noDrag: function (element) {
          return element.attr('no-drag') !== undefined || element.attr('data-no-drag') !== undefined;
        }
      };
    }
  ]);

}());