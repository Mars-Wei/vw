#include<iostream>
using namespace std;
size_t  unpack(const char * src_begin, const char * src_end, char * dst_begin, char * dst_end, int common_size, int item_size)
{// common \n item \n item \n
     const char * common_b = src_begin;
     const char * item_b = src_begin + common_size;

     char * p = dst_begin;

     while(item_b != src_end)
     {
        if(p+common_size > dst_end)
            break;
         memcpy(p, common_b, common_size-1);
         p += common_size - 1;
         const char * item_e = item_b;
         while( *item_e != '\n' && item_e != src_end )
             item_e++;
        if(p + (item_e+1-item_b) > dst_end)
            break;
         memcpy(p, item_b, item_e+1-item_b);
         p += (item_e+1-item_b);
         item_b = item_e+1;
     }
     return p-dst_begin;
}

int main()
{
    int buf_size = 49;
    char * dst_b = new char[buf_size];
    char * dst_e = dst_b+buf_size;
    memset( dst_b, 0, buf_size);
    const char * src_b = "|a 1 2 3|b 4 5 6\n|c 1 2 3\n|d 1 2 3\n";
    const char * src_e=src_b+strlen(src_b);

    cout<<unpack(src_b, src_e, dst_b, dst_e, 17, strlen(src_b)-17)<<endl;

    cout<<dst_b<<endl;
}
