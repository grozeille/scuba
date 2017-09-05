module.exports = routesConfig;

/** @ngInject */
function routesConfig($stateProvider, $urlRouterProvider) {
  $stateProvider
    .state('dataset', {
      url: '/dataset',
      component: 'datasetControllerComponent'
    })
    .state('datasetEditor', {
      url: '/dataset/editor',
      component: 'datasetEditorControllerComponent',
      params: {
        scrollTo: 'body'
      }
    })
    .state('chooseTable', {
      url: '/dataset/chooseTable',
      component: 'chooseTableControllerComponent'
    })
    .state('catalog', {
      url: '/catalog',
      component: 'catalogControllerComponent',
      params: {
        scrollTo: 'body'
      }
    })
    .state('customFileDataSet', {
      url: '/dataset/customFile',
      component: 'customFileDataSetControllerComponent'
    })
    .state('admin', {
      url: '/admin',
      component: 'adminControllerComponent'
    })
    .state('profile', {
      url: '/profile',
      component: 'profileControllerComponent'
    })
    .state('setup', {
      url: '/setup',
      component: 'setupControllerComponent'
    })
    .state('project', {
      url: '/project/:id',
      component: 'projectControllerComponent'
    })
    ;

  $urlRouterProvider.otherwise('/dataset');
}
