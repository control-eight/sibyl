package com.my.sibyl.itemsets.score_function;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class ScoreFunctionResult<T, S> {

    private T result;

    private S score;

    public ScoreFunctionResult(T result, S score) {
        this.result = result;
        this.score = score;
    }

    public T getResult() {
        return result;
    }

    public S getScore() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScoreFunctionResult that = (ScoreFunctionResult) o;

        if (result != null ? !result.equals(that.result) : that.result != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return result != null ? result.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ScoreFunctionResult{" +
                "result=" + result +
                ", score=" + score +
                '}';
    }
}
