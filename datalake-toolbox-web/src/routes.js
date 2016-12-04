module.exports = routesConfig;

/** @ngInject */
function routesConfig($stateProvider, $urlRouterProvider, $locationProvider) {
  $locationProvider.html5Mode(true).hashPrefix('!');
  $urlRouterProvider.otherwise('/');

  $stateProvider
    .state('view', {
      url: '/',
      component: 'viewControllerComponent'
    })
    .state('viewEditor', {
      url: '/editor',
      component: 'viewEditorControllerComponent',
      params: {
        scrollTo: 'body'
      }
    })
    .state('chooseTable', {
      url: '/chooseTable',
      component: 'chooseTableControllerComponent'
    })
    .state('catalog', {
      url: '/catalog',
      component: 'catalogControllerComponent',
      params: {
        scrollTo: 'body'
      }
    })
    .state('tableEditor', {
      url: '/tableEditor/:database/:table',
      component: 'tableEditorControllerComponent',
      params: {
        scrollTo: 'body'
      }
    });

  $urlRouterProvider.otherwise('/');
}
