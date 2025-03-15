// Path Syntax kuch aysy ha foldername/filename agr file kisi or foder main ha to tb uska path likhna hota ha
#include "Database.h"
using namespace std;

int main()
{
    Database db;

    db.createTable("Student");
    db.createTable("Teacher");

    // Adding Columns for Student
    db.addColumnToTable("Student", "RollNo", "int", true);
    db.addColumnToTable("Student", "Name", "string", false);
    db.addColumnToTable("Student", "Age", "int", false);

    // Adding Columns for Teacher
    db.addColumnToTable("Teacher", "TeacherID", "int", true);
    db.addColumnToTable("Teacher", "Name", "string", false);
    db.addColumnToTable("Teacher", "Subject", "string", false);

    // Display Schema
    db.showTableSchema("Student");
    db.showTableSchema("Teacher");

    return 0;
}
