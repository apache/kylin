/*jshint indent: 2 */
/*global angular: false */

(function () {

  'use strict';
  var mainModule = angular.module('ui.sortable');

  /**
   * Controller for sortableItemHandle
   *
   * @param $scope - item handle scope.
   */
  mainModule.controller('ui.sortable.sortableItemHandleController', ['$scope', function ($scope) {

    this.scope = $scope;

    $scope.itemScope = null;
    $scope.type = 'handle';
  }]);

  /**
   * Directive for sortable item handle.
   */
  mainModule.directive('asSortableItemHandle', ['sortableConfig', '$helper', '$window', '$document',
    function (sortableConfig, $helper, $window, $document) {
      return {
        require: '^asSortableItem',
        scope: true,
        restrict: 'A',
        controller: 'ui.sortable.sortableItemHandleController',
        link: function (scope, element, attrs, itemController) {

          var dragElement, //drag item element.
            placeHolder, //place holder class element.
            placeElement,//hidden place element.
            itemPosition, //drag item element position.
            dragItemInfo, //drag item data.
            containment,//the drag container.
            containerPositioning, // absolute or relative positioning.
            dragListen,// drag listen event.
            scrollableContainer, //the scrollable container
            dragStart,// drag start event.
            dragMove,//drag move event.
            dragEnd,//drag end event.
            dragCancel,//drag cancel event.
            isDraggable,//is element draggable.
            isDragBefore,//is element moved up direction.
            isPlaceHolderPresent,//is placeholder present.
            bindDrag,//bind drag events.
            unbindDrag,//unbind drag events.
            bindEvents,//bind the drag events.
            unBindEvents,//unbind the drag events.
            hasTouch,// has touch support.
            dragHandled, //drag handled.
            isDisabled = false; // drag enabled

          hasTouch = $window.hasOwnProperty('ontouchstart');

          if (sortableConfig.handleClass) {
            element.addClass(sortableConfig.handleClass);
          }

          scope.itemScope = itemController.scope;

          scope.$watch('sortableScope.isDisabled', function(newVal) {
            if(isDisabled !== newVal) {
              isDisabled = newVal;
              if(isDisabled) {
                unbindDrag();
              } else {
                bindDrag();
              }
            }
          });

          /**
          * Listens for a 10px movement before
          * dragStart is called to allow for
          * a click event on the element.
          *
          * @param event - the event object.
          */
          dragListen = function (event) {

            var unbindMoveListen = function () {
              angular.element($document).unbind('mousemove', moveListen);
              angular.element($document).unbind('touchmove', moveListen);
              element.unbind('mouseup', unbindMoveListen);
              element.unbind('touchend', unbindMoveListen);
              element.unbind('touchcancel', unbindMoveListen);
            };
            
            var startPosition;
            var moveListen = function (e) {
              e.preventDefault();
              var eventObj = $helper.eventObj(e);
              if (!startPosition) {
                startPosition = { clientX: eventObj.clientX, clientY: eventObj.clientY };
              }
              if (Math.abs(eventObj.clientX - startPosition.clientX) + Math.abs(eventObj.clientY - startPosition.clientY) > 10) {
                unbindMoveListen();
                dragStart(event);
              }
            };
            
            angular.element($document).bind('mousemove', moveListen);
            angular.element($document).bind('touchmove', moveListen);
            element.bind('mouseup', unbindMoveListen);
            element.bind('touchend', unbindMoveListen);
            element.bind('touchcancel', unbindMoveListen);
          };

          /**
           * Triggered when drag event starts.
           *
           * @param event the event object.
           */
          dragStart = function (event) {

            var eventObj, tagName;

            if (!hasTouch && (event.button === 2 || event.which === 3)) {
              // disable right click
              return;
            }
            if (hasTouch && $helper.isTouchInvalid(event)) {
              return;
            }
            if (dragHandled || !isDraggable(event)) {
              // event has already fired in other scope.
              return;
            }
            // Set the flag to prevent other items from inheriting the drag event
            dragHandled = true;
            event.preventDefault();
            eventObj = $helper.eventObj(event);

            // (optional) Scrollable container as reference for top & left offset calculations, defaults to Document
            scrollableContainer = angular.element($document[0].querySelector(scope.sortableScope.options.scrollableContainer)).length > 0 ?
              $document[0].querySelector(scope.sortableScope.options.scrollableContainer) : $document[0].documentElement;

            containment = angular.element($document[0].querySelector(scope.sortableScope.options.containment)).length > 0 ?
              angular.element($document[0].querySelector(scope.sortableScope.options.containment)) : angular.element($document[0].body);
            //capture mouse move on containment.
            containment.css('cursor', 'move');

            // container positioning
            containerPositioning = scope.sortableScope.options.containerPositioning || 'absolute';

            dragItemInfo = $helper.dragItem(scope);
            tagName = scope.itemScope.element.prop('tagName');

            dragElement = angular.element($document[0].createElement(scope.sortableScope.element.prop('tagName')))
              .addClass(scope.sortableScope.element.attr('class')).addClass(sortableConfig.dragClass);
            dragElement.css('width', $helper.width(scope.itemScope.element) + 'px');
            dragElement.css('height', $helper.height(scope.itemScope.element) + 'px');

            placeHolder = angular.element($document[0].createElement(tagName))
                .addClass(sortableConfig.placeHolderClass).addClass(scope.sortableScope.options.additionalPlaceholderClass);
            placeHolder.css('width', $helper.width(scope.itemScope.element) + 'px');
            placeHolder.css('height', $helper.height(scope.itemScope.element) + 'px');

            placeElement = angular.element($document[0].createElement(tagName));
            if (sortableConfig.hiddenClass) {
              placeElement.addClass(sortableConfig.hiddenClass);
            }

            itemPosition = $helper.positionStarted(eventObj, scope.itemScope.element, scrollableContainer);
            //fill the immediate vacuum.
            scope.itemScope.element.after(placeHolder);
            //hidden place element in original position.
            scope.itemScope.element.after(placeElement);
            dragElement.append(scope.itemScope.element);

            containment.append(dragElement);
            $helper.movePosition(eventObj, dragElement, itemPosition, containment, containerPositioning, scrollableContainer);

            scope.sortableScope.$apply(function () {
              scope.callbacks.dragStart(dragItemInfo.eventArgs());
            });
            bindEvents();
          };

          /**
           * Allow Drag if it is a proper item-handle element.
           *
           * @param event - the event object.
           * @return boolean - true if element is draggable.
           */
          isDraggable = function (event) {

            var elementClicked, sourceScope, isDraggable;

            elementClicked = angular.element(event.target);
            sourceScope = elementClicked.scope();

            // look for the handle on the current scope or parent scopes
            isDraggable = false;
            while (!isDraggable && sourceScope !== undefined) {
              if (sourceScope.type && sourceScope.type === 'handle') {
                isDraggable = true;
              } else {
                sourceScope = sourceScope.$parent;
              }
            }

            //If a 'no-drag' element inside item-handle if any.
            while (isDraggable && elementClicked[0] !== element[0]) {
              if ($helper.noDrag(elementClicked)) {
                isDraggable = false;
              }
              elementClicked = elementClicked.parent();
            }
            return isDraggable;
          };

          /**
           * Inserts the placeHolder in to the targetScope.
           *
           * @param targetElement the target element
           * @param targetScope the target scope
           */
          function insertBefore(targetElement, targetScope) {
            targetElement[0].parentNode.insertBefore(placeHolder[0], targetElement[0]);
            dragItemInfo.moveTo(targetScope.sortableScope, targetScope.index());
          }

          /**
           * Inserts the placeHolder next to the targetScope.
           *
           * @param targetElement the target element
           * @param targetScope the target scope
           */
          function insertAfter(targetElement, targetScope) {
            targetElement.after(placeHolder);
            dragItemInfo.moveTo(targetScope.sortableScope, targetScope.index() + 1);
          }

          /**
           * Triggered when drag is moving.
           *
           * @param event - the event object.
           */
          dragMove = function (event) {

            var eventObj, targetX, targetY, targetScope, targetElement;

            if (hasTouch && $helper.isTouchInvalid(event)) {
              return;
            }
            // Ignore event if not handled
            if (!dragHandled) {
              return;
            }
            if (dragElement) {

              event.preventDefault();

              eventObj = $helper.eventObj(event);
              $helper.movePosition(eventObj, dragElement, itemPosition, containment, containerPositioning, scrollableContainer);

              targetX = eventObj.pageX - $document[0].documentElement.scrollLeft;
              targetY = eventObj.pageY - ($window.pageYOffset || $document[0].documentElement.scrollTop);

              //IE fixes: hide show element, call element from point twice to return pick correct element.
              dragElement.addClass(sortableConfig.hiddenClass);
              $document[0].elementFromPoint(targetX, targetY);
              targetElement = angular.element($document[0].elementFromPoint(targetX, targetY));
              dragElement.removeClass(sortableConfig.hiddenClass);

              targetScope = targetElement.scope();

              if (!targetScope || !targetScope.type) {
                return;
              }
              if (targetScope.type === 'handle') {
                targetScope = targetScope.itemScope;
              }
              if (targetScope.type !== 'item' && targetScope.type !== 'sortable') {
                return;
              }

              if (targetScope.type === 'item') {
                targetElement = targetScope.element;
                if (targetScope.sortableScope.accept(scope, targetScope.sortableScope, targetScope)) {
                  if (itemPosition.dirAx && //move horizontal
                    scope.itemScope.sortableScope.$id === targetScope.sortableScope.$id) { //move same column
                    itemPosition.distAxX = 0;
                    if (itemPosition.distX < 0) {//move left
                      insertBefore(targetElement, targetScope);
                    } else if (itemPosition.distX > 0) {//move right
                      insertAfter(targetElement, targetScope);
                    }
                  } else { //move vertical
                    if (isDragBefore(eventObj, targetElement)) {//move up
                      insertBefore(targetElement, targetScope);
                    } else {//move bottom
                      insertAfter(targetElement, targetScope);
                    }
                  }
                }
              }
              if (targetScope.type === 'sortable') {//sortable scope.
                if (targetScope.accept(scope, targetScope) &&
                  targetElement[0].parentNode !== targetScope.element[0]) {
                  //moving over sortable bucket. not over item.
                  if (!isPlaceHolderPresent(targetElement)) {
                    //append to bottom.
                    targetElement[0].appendChild(placeHolder[0]);
                    dragItemInfo.moveTo(targetScope, targetScope.modelValue.length);
                  }
                }
              }
            }
          };

          /**
           * Check there is no place holder placed by itemScope.
           * @param targetElement the target element to check with.
           * @returns {*} true if place holder present.
           */
          isPlaceHolderPresent = function (targetElement) {
            var itemElements, hasPlaceHolder, i;

            itemElements = targetElement.children();
            for (i = 0; i < itemElements.length; i += 1) {
              if (angular.element(itemElements[i]).hasClass(sortableConfig.placeHolderClass)) {
                hasPlaceHolder = true;
                break;
              }
            }
            return hasPlaceHolder;
          };


          /**
           * Determines whether the item is dragged upwards.
           *
           * @param eventObj - the event object.
           * @param targetElement - the target element.
           * @returns {boolean} - true if moving upwards.
           */
          isDragBefore = function (eventObj, targetElement) {
            var dragBefore, targetOffset;

            dragBefore = false;
            // check it's new position
            targetOffset = $helper.offset(targetElement);
            if ($helper.offset(placeHolder).top > targetOffset.top) { // the move direction is up?
              dragBefore = $helper.offset(dragElement).top < targetOffset.top + $helper.height(targetElement) / 2;
            } else {
              dragBefore = eventObj.pageY < targetOffset.top;
            }
            return dragBefore;
          };

          /**
           * Rollback the drag data changes.
           */

          function rollbackDragChanges() {
            placeElement.replaceWith(scope.itemScope.element);
            placeHolder.remove();
            dragElement.remove();
            dragElement = null;
            dragHandled = false;
            containment.css('cursor', '');
          }

          /**
           * triggered while drag ends.
           *
           * @param event - the event object.
           */
          dragEnd = function (event) {
            // Ignore event if not handled
            if (!dragHandled) {
              return;
            }
            event.preventDefault();
            if (dragElement) {
              //rollback all the changes.
              rollbackDragChanges();
              // update model data
              dragItemInfo.apply();
              scope.sortableScope.$apply(function () {
                if (dragItemInfo.isSameParent()) {
                  if (dragItemInfo.isOrderChanged()) {
                    scope.callbacks.orderChanged(dragItemInfo.eventArgs());
                  }
                } else {
                  scope.callbacks.itemMoved(dragItemInfo.eventArgs());
                }
              });
              scope.sortableScope.$apply(function () {
                scope.callbacks.dragEnd(dragItemInfo.eventArgs());
              });
              dragItemInfo = null;
            }
            unBindEvents();
          };

          /**
           * triggered while drag is cancelled.
           *
           * @param event - the event object.
           */
          dragCancel = function (event) {
            // Ignore event if not handled
            if (!dragHandled) {
              return;
            }
            event.preventDefault();

            if (dragElement) {
              //rollback all the changes.
              rollbackDragChanges();
              scope.sortableScope.$apply(function () {
                scope.callbacks.dragCancel(dragItemInfo.eventArgs());
              });
              dragItemInfo = null;
            }
            unBindEvents();
          };

          /**
           * Binds the drag start events.
           */
          bindDrag = function () {
            element.bind('touchstart', dragListen);
            element.bind('mousedown', dragListen);
          };

          /**
           * Unbinds the drag start events.
           */
          unbindDrag = function () {
            element.unbind('touchstart', dragListen);
            element.unbind('mousedown', dragListen);
          };

          //bind drag start events.
          bindDrag();

          //Cancel drag on escape press.
          angular.element($document[0].body).bind('keydown', function (event) {
            if (event.keyCode === 27) {
              dragCancel(event);
            }
          });

          /**
           * Binds the events based on the actions.
           */
          bindEvents = function () {
            angular.element($document).bind('touchmove', dragMove);
            angular.element($document).bind('touchend', dragEnd);
            angular.element($document).bind('touchcancel', dragCancel);
            angular.element($document).bind('mousemove', dragMove);
            angular.element($document).bind('mouseup', dragEnd);
          };

          /**
           * Un binds the events for drag support.
           */
          unBindEvents = function () {
            angular.element($document).unbind('touchend', dragEnd);
            angular.element($document).unbind('touchcancel', dragCancel);
            angular.element($document).unbind('touchmove', dragMove);
            angular.element($document).unbind('mouseup', dragEnd);
            angular.element($document).unbind('mousemove', dragMove);
          };
        }
      };
    }]);
}());
