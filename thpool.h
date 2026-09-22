#ifndef THPOOL_H
#define THPOOL_H

// 只声明 thpool 类型，隐藏线程池内部结构
typedef struct thpool thpool;

// 创建线程池
thpool *thpool_init(int threads_num);

// 向线程池提交任务
int thpool_add_work(thpool *pool,void (*func)(void *),void *arg);

#endif