#include "thpool.h"
#include <pthread.h>

// 任务 - 设计
typedef struct job{
    // 与其他任务的连接
    struct job * next;

    // 执行函数
    void(*func)(void *); // 通用地址void *

    // 参数地址
    void *arg;
}job;

// 任务队列 - 设计
typedef struct jobqueue{

    // 互斥锁
    pthread_mutex_t mutex;
    // 条件变量
    pthread_cond_t has_cond;
    // 队头指针
    job *front;
    // 队尾指针
    job *rear;
    // 队长
    int len;
    
}jobqueue;

// 任务队列初始化
int jobqueue_init(jobqueue *p){
    if(pthread_mutex_init(&(p->mutex),NULL) != 0){
        return -1;
    }
    if(pthread_cond_init(&(p->has_cond),NULL) != 0){
        pthread_mutex_destroy(&(p->mutex)); // 初始化条件变量失败 需要销毁已经创建的互斥锁
        return -1;
    }
    p->front = NULL;
    p->rear = NULL;
    p->len = 0;

    return 0;
}

// 任务队列 - 放任务
void jobqueue_push(jobqueue *queue,job *newjob){
    // 队列加锁
    pthread_mutex_lock(&(queue->mutex));
    // 添加任务到队尾
    newjob->next = NULL;
    if(queue->len == 0){    // 空队列
        queue->front = newjob;
        queue->rear = newjob;

    }
    else{
        queue->rear->next = newjob;
        queue->rear = newjob;
    }
    queue->len++;
    pthread_cond_signal(&(queue->has_cond));   // 唤醒一个阻塞的线程
    // 队列解锁
    pthread_mutex_unlock(&(queue->mutex));
}

// 任务队列 - 取任务
job *jobqueue_pull(jobqueue *queue){
    // 队列加锁
    pthread_mutex_lock(&(queue->mutex));

    // 从队头取任务
    if(queue->len == 0){    // 空队列
        pthread_mutex_unlock(&(queue->mutex));
        return NULL;    // 结束之前要注意有没有解锁 不然这把锁就没人解锁了
    }

    // 临时指针 存放第一个任务的地址
    job *temp_job = queue->front;
    if(queue->front == queue->rear){   // 队列只有一个任务
        queue->front = NULL;
        queue->rear = NULL;
    }
    else{ // 队列任务>1
        queue->front = queue->front->next;
    }
    // 取完任务 任务个数减一
    queue->len--;
    
    // 队列解锁
    pthread_mutex_unlock(&(queue->mutex));

    // 取出的任务 next应该置空 彻底脱离链表队列 并且已经不属于队列,修改不要占用任务队列的mutex 减少占用锁的时间
    // 临界区尽量短 把需要保护的共享资源操作放到锁里面
    temp_job->next = NULL;

    return temp_job;
}