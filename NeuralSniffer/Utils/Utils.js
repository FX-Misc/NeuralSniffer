"use strict";
//As an example, in normal JavaScript, mistyping a variable name creates a new global variable. In strict mode, this will throw an error, making it impossible to accidentally create a global variable.

// Create a stopwatch "class." 
var StopWatch = function () {
    this.StartMilliseconds = 0;
    this.ElapsedMilliseconds = 0;
}

StopWatch.prototype.Start = function () {
    this.StartMilliseconds = new Date().getTime();
}

StopWatch.prototype.GetTimestampInMsec = function () {
    this.ElapsedMilliseconds = new Date().getTime() - this.StartMilliseconds;
    return this.ElapsedMilliseconds;
}