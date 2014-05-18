#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include<sys/socket.h>
#include<sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include<limits.h>
#include<fcntl.h>
#include<unistd.h>
#include<sys/time.h>
using namespace std;
int send_with_timeout(int sock, const void *ptr, size_t nbytes, int msecs)
{
    struct timeval tv;
    struct timeval old_tv;
    ssize_t nwrite = 0;
    int ret = 0;
    int sockflag = 0;
    int timeuse = 0;
    socklen_t oplen = sizeof(tv);

    nwrite = send(sock, ptr, nbytes, MSG_DONTWAIT);
    if ((nwrite == (ssize_t)nbytes)) {
        return nwrite;
    }
    if ((nwrite < 0) && (errno != EAGAIN) &&
            (errno != EWOULDBLOCK) && (errno != EINTR)){
        return -1;
    }
    if (nwrite < 0) {
        nwrite = 0;
    }

    if (0 == msecs) {
        errno = ETIMEDOUT;
        return -1;
    }

    if (msecs < 0) {
        msecs = INT_MAX;
    }

    if (getsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &old_tv, &oplen) < 0) {
        return -1;
    }
    if ((sockflag = fcntl(sock, F_GETFL, 0)) < 0){
        return -1;
    }

    //close NONBLOCK flag
    if (sockflag&O_NONBLOCK) {
        if (fcntl(sock, F_SETFL, (sockflag)&(~O_NONBLOCK)) < 0) {
            return -1;
        }
    }
    oplen = sizeof(tv);
    struct timeval cur;
    struct timeval last;
    do {
        tv.tv_sec = msecs/1000;
        tv.tv_usec = (msecs%1000)*1000;
        gettimeofday(&cur, NULL);
        if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, oplen) < 0){
            return -1;
        }
        do {
            ret = send(sock, (char*)ptr + nwrite, nbytes-(size_t)nwrite, MSG_WAITALL);
        } while (ret < 0 && EINTR == errno);

        if (ret > 0 && nwrite+ret < (ssize_t)nbytes) {
            gettimeofday(&last, NULL);
            timeuse = ((last.tv_usec - cur.tv_usec)/1000+(last.tv_sec - cur.tv_sec)*1000);
            if (timeuse >= msecs) {
                errno = ETIMEDOUT;
                ret = -1;
                nwrite = -1;
            } else {
                msecs -= timeuse;
                nwrite += ret;

            }
        } else if (ret < 0) {
            nwrite = -1;//write with error
        } else {
            nwrite += ret;//write all
        }
    } while (ret > 0 && nbytes != (size_t)nwrite);

    //reset to old flag
    if (sockflag & O_NONBLOCK) {
        if (fcntl(sock, F_SETFL, sockflag) < 0) {
            return -1;
        }
    }
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &old_tv, oplen) < 0) {
        return -1;
    }

    return nwrite;
}


int get_req_size(const char * pstr, size_t size)
{
    int num  = 0;
    const char * p = pstr;
    while(p != pstr+size)
    {
        if(*p == '\n')
        {
            num += 1;
        }
        p++;
    }

    return num;
}

ssize_t read_with_timeout(int sock, void *ptr, size_t nreq, int msecs)
{
    struct timeval tv;
    struct timeval old_tv;
    size_t nread = 0;
    size_t curnum = 0;
    int sockflag;
    int ret;
    int timeuse = 0;
    socklen_t oplen = (socklen_t)sizeof(tv);
    if (msecs < 0) {
        msecs = INT_MAX;
    }
    if (getsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &old_tv, &oplen) <0) {
        return -1;
    }
    if ((sockflag = fcntl(sock, F_GETFL, 0)) < 0) {
        return -1;
    }
    if (sockflag&O_NONBLOCK) {
        if (fcntl(sock, F_SETFL, (sockflag)&(~O_NONBLOCK)) < 0) {
            return -1;
        }
    }

    struct timeval cur;
    struct timeval last;
    do {
        cout << "enter read in while loop" <<endl;
        tv.tv_sec = msecs/1000;
        tv.tv_usec = (msecs%1000)*1000;
        if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, oplen) < 0) {
            cout << "set wait rec time error." <<endl;
            return -1;
        }
        gettimeofday(&cur, NULL);
        do {
            ret = recv(sock, (char*)ptr+nread, 10240, 0/*MSG_WAITALL*/);
            cout << " read ret "<< ret <<endl;
        } while (ret < 0 && errno == EINTR);
        if (ret<0 && EAGAIN==errno) {
            errno = ETIMEDOUT;
        }
        if(ret > 0)
        {//read some data
           curnum += get_req_size((char*)ptr+nread, ret);
            if(curnum != nreq){
                gettimeofday(&last, NULL);
                timeuse = ((last.tv_usec - cur.tv_usec)/1000+(last.tv_sec - cur.tv_sec)*1000);
                if (timeuse >= msecs) {
                    cout << "time out : " << timeuse << " msecs " << msecs <<endl;
                    errno = ETIMEDOUT;
                    ret = -1;
                    nread = -1;
                } else {
                    nread += ret;
                }
            }
        } else if (ret < 0) {
            nread = -1;//read error
        } 
    } while (ret > 0 && curnum != nreq );
    if (sockflag&O_NONBLOCK){
        if (fcntl(sock, F_SETFL, sockflag) < 0){
            cout << "return noblock . f_setfl."<<endl;
            return -1;
        }
    }
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &old_tv, oplen) < 0){
        return -1;
    }
    return curnum;
}

int main(int argc, char * argv[])
{

    cout<<"using addr "<<argv[1]<<endl;
    int sockid = socket(AF_INET, SOCK_STREAM, 0);
    if(sockid<0)
    {
        cerr << "create sock failed" <<endl;
        exit(1);
    }

    //connect
    struct sockaddr_in serv_addr;
    bzero(&serv_addr,sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    if(inet_aton(argv[1],&serv_addr.sin_addr) == 0) //服务器的IP地址来自程序的参数
    {
        printf("Server IP Address Error!\n");
        exit(1);
    }
    serv_addr.sin_port = htons(8888);
    socklen_t serv_addr_len = sizeof(serv_addr);
    if(connect(sockid, (struct sockaddr *)&serv_addr, serv_addr_len))
    {
        cerr << "connect failed" <<endl;
        exit(1);
    }


    //send 
    const char * pmsg = " |a aaa sss|b ss ss ss s ss s s s s s s s s s s s s s s a a 3s s s s s s s s s s s s s s s s s s s ss s s s ss |k ss ss ss9 sds sdfs sss ssss ss sssssssssss|c ssss ss  |d aaa sss|e ss ss ss s ss s s s s s s s s s s s s s s a a 3s s s s s s s s s s s s s s s s s s s ss s s s ss |f ss ss ss9 sds sdfs sss ssss ss sssssssssss|g ssss ss\n";
    const char * pmsg2 = " |a aaa sss|b ss ss ss s ss s s s s s s s s s s s s s s a a 3s s s s s s s s s s s s s s s s s s s ss s s s ss |k ss ss ss9 sds sdfs sss ssss ss sssssssssss|c ssss ss  |d aaa sss|e ss ss ss s ss s s s s s s s s s s s s s s a a 3s s s s s s s s s s s s s s s s s s s ss s s s ss |f ss ss ss9 sds sdfs sss ssss ss sssssssssss|g ssss sss";
    size_t msg_len = strlen(pmsg);
    cout<<msg_len<<endl;
    if(msg_len != send_with_timeout(sockid, pmsg, strlen(pmsg), 10))
    {
        cerr << "send error 1" <<endl;
        exit(1);
    }
    if(msg_len != send_with_timeout(sockid, pmsg2, strlen(pmsg2), 10))
    {
        cerr << "send error 2" <<endl;
        exit(1);
    }

    char * pbuf = new char[102400];
    memset(pbuf, 0, 102400);
    int ret = read_with_timeout(sockid, pbuf, 2, 1000);
    if( 2!= ret)
    {
        cerr << "read error" <<endl;
        exit(1);
    }
    else{
        cout<<"end ok!"<<endl;
        cout<<pbuf<<endl;
    }

    return 0;

}
