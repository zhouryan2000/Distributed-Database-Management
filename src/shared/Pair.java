package shared;

public class Pair<Type1, Type2> {
    public Type1 fst;
    public Type2 snd;

    Pair() {
    }

    Pair(Type1 a, Type2 b) {
        this.fst = a;
        this.snd = b;
    }

    public Type1 getFirst() {
        return fst;
    }

    public Type2 getSecond() {
        return snd;
    }
}
