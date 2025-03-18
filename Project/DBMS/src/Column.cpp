#include"Column.h"
#include <iostream>
using namespace std;

// Column::Column(const string& name,const string& type, bool isprimarykey):name(name),type(type),isprimarykey(isprimarykey){}


Column::Column(string name, string type, bool isPrimaryKey)
    : name(name), type(type), isPrimaryKey(isPrimaryKey) {}

// void Column::displayColumns() const
// {
//     cout<<"Column Name: "<<name<<" | Type:"<<type<<endl;
//     if(isprimarykey)
//     {
//         cout<<" | Primary Key"<<endl;
//     } 
// }

