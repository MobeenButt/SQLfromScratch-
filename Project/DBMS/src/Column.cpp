#include"Column.h"
#include <iostream>
using namespace std;

<<<<<<< Updated upstream
Column::Column(const string& name,const string& type, bool isprimarykey):name(name),type(type),isprimarykey(isprimarykey){}
=======
// Column::Column(const string& name,const string& type, bool isprimarykey):name(name),type(type),isprimarykey(isprimarykey){}


Column::Column(string name, string type, bool isprimarykey)
    : name(name), type(type), isprimarykey(isprimarykey) {} // ✅ Correct field name
string Column::getName()const
{
    return name;
}
bool Column::isPrimaryKey()const
{
    return isprimarykey;
}
string Column::getType()const
{
    return type;
}
// void Column::displayColumns() const
// {
//     cout<<"Column Name: "<<name<<" | Type:"<<type<<endl;
//     if(isprimarykey)
//     {
//         cout<<" | Primary Key"<<endl;
//     } 
// }
>>>>>>> Stashed changes

void Column::displayColumns() const
{
    cout<<"Column Name: "<<name<<" | Type:"<<type<<endl;
    if(isprimarykey)
    {
        cout<<" | Primary Key"<<endl;
    } 
}