(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .config(routerConfig);

  /** @ngInject */
  function routerConfig($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('view', {
         url: '/',
         templateUrl: 'app/view/view.html',
         controller: 'ViewController',
         controllerAs: 'view',
         data: {
             css: 'app/view/view.css'
         },
         params: {
           scrollTo: 'body'
         }
      })
      .state('viewEditor', {
        url: '/editor',
        templateUrl: 'app/viewEditor/viewEditor.html',
        controller: 'ViewEditorController',
        controllerAs: 'viewEditor',
        data: {
            css: 'app/viewEditor/viewEditor.css'
        },
        params: {
          scrollTo: 'body'
        }
      })
      .state('viewEditor.links', {
        parent: 'viewEditor',
        url: '/links/:database/:table',
        modal: true,
        size: 'lg',
        templateUrl: 'app/viewEditor/links/links.html',
        controller: 'LinksController',
        controllerAs: 'links'
      })
      .state('chooseTable', {
        url: '/chooseTable',
        templateUrl: 'app/chooseTable/chooseTable.html',
        controller: 'ChooseTableController',
        controllerAs: 'chooseTable',
        data: {
            css: 'app/chooseTable/chooseTable.css'
        }
      })
      .state('catalog', {
        url: '/catalog',
        templateUrl: 'app/catalog/catalog.html',
        controller: 'CatalogController',
        controllerAs: 'catalog',
        data: {
            css: 'app/catalog/catalog.css'
        },
        params: {
          scrollTo: 'body'
        }
      })
      .state('tableEditor', {
        url: '/tableEditor',
        templateUrl: 'app/tableEditor/tableEditor.html',
        controller: 'TableEditorController',
        controllerAs: 'tableEditor',
        data: {
            css: 'app/tableEditor/tableEditor.css'
        },
        params: {
          scrollTo: 'body'
        }
      });

    $urlRouterProvider.otherwise('/');
  }

})();
