#ifndef COULMN_H
#define COULMN_H

#include<string>
using namespace std;

class Column{
private:
    string name;
    string type;
    bool isprimarykey;

public:
// Column(const string& name,const string& type, bool isprimarykey=false);
// Column()=default;
Column(const string& name = "", const string& type = "", bool isprimarykey = false);

void displayColumns() const;
};
#endif 


// indef means if the file is not defined then define it
