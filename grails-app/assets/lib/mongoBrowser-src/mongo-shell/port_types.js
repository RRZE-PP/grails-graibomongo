
Timestamp.prototype.toDateString = function(){
	return new Date(this.low_ * 1000).toString();
}

Timestamp.prototype.toString = function() {
	return "Timestamp(" + this.low_ + ", " + this.high_ + ")"
}

Timestamp.prototype.getTime = function() {
    return this.low_;
};

Timestamp.prototype.getInc = function() {
    return this.high_;
};
