class BigsetAccumulator {
emit = false;
currentElement;
currentCtx = new BigsetClock();
currentDots = new Dots();

readyKey;
readyValue;

add(k, v) {
  parsedKey = parseKey(k);
  
  if(parsedKey.element != currentElement) {

    Dots remaining = currentCtx->subract_seen(currentDots);

    if (remaining.length()) > 0) {
    //this element is "in" the set locally
    readyKey = currentElement;
    readyValue = remaing;
    ready = true;
    currentCtx.reset();
    currentDots.reset();
  }
}

// accumulate values
currentElement = parseKey.element;
currentCtx = currentCtx.merge(valueToClock(v));
currentDots.add(parseKey.actor, parsedKey.counter. parsedKey.tombstone);
}


emit() {
  return emit;
}

getReadyKey() {
  return readyKey;
}

getReadyValue() {
  return readyValue;
}

taken() {
  emit=false;
}

}

class Dots {
  list dots = new List();
  add(actor, counter, tsb) {
    if(tsb == add) {
      dots.append({actor, counter});
    }
  }

}

// Erm, this is more than I can do on a train with 20 minutes left, sorry.
// Will sketch it out on Thursday
  class BigsetClock {
    merge(clock) {
    }

    subtractSeen(dots) {
    }
  }
    
  

