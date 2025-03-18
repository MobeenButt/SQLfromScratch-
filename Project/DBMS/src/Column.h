#ifndef COLUMN_H
#define COLUMN_H

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
<<<<<<< Updated upstream
Column(const string& name = "", const string& type = "", bool isprimarykey = false);

void displayColumns() const;
=======
// Column(const string& name = "", const string& type = "", bool isprimarykey = false);
Column(string name, string type, bool isPrimaryKey);
string getName()const;
bool isPrimaryKey()const;
string getType()const;

<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
};
#endif 


// indef means if the file is not defined then define it
