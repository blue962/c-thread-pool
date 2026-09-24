#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

#include "thpool.h"

void test_task(void *arg){
    int num = *(int *)arg;
    printf("线程 %lu 正在执行任务 %d\n",(unsigned long)pthread_self(),num);
    sleep(1);
}

int main(void){
    thpool *pool = thpool_init(4);

    if(pool == NULL){
        printf("线程池创建失败\n");
        return 1;
    }
    int nums[10];

    // 提交 10 个测试任务
    for(int i = 0; i < 10; i++){

        nums[i] = i;

        if(thpool_add_work(pool, test_task, &nums[i]) != 0){
            printf("任务 %d 提交失败\n", i);
        }
    }

    // 临时等待工作线程执行任务
    thpool_wait(pool);

    return 0;
}