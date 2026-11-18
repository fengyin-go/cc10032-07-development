import { createRouter, createWebHistory } from 'vue-router'

import JoinWorkbench from '@/views/JoinWorkbench.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 序号7流关联：带关联结果版本控制台的专用工作台，须先于动态模块路由匹配
    { path: '/join_stream', name: 'join-workbench', component: JoinWorkbench },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
