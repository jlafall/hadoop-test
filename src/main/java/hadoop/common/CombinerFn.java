package hadoop.common;

import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public abstract class CombinerFn<K, V> extends CombineFn<K, V> {
	
	@Override
	public void process(Pair<K,Iterable<V>> input,  Emitter<Pair<K,V>> emitter) {
		emitter.emit(new Pair(input.first(), combine(input.second())));
	}

	public abstract V combine(Iterable<V> iterable);
}