using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;

namespace HQCommon
{
    public static partial class Utils
    {
        /// <summary> Throws AggregateException if more than one fails. </summary>
        public static void DisposeAll(System.Collections.IEnumerable p_seq)
        {
            if (p_seq == null)
                return;
            List<Exception> errors = null;
            System.Collections.IEnumerator it;
            using ((it = p_seq.GetEnumerator()) as IDisposable)
                while (it.MoveNext())
                {
                    try
                    {
                        IDisposable d = it.Current as IDisposable;
                        if (d != null)
                            d.Dispose();
                    }
                    catch (Exception e)
                    {
                        if (errors == null)
                            errors = new List<Exception> { e };
                        else
                            errors.Add(e);
                    }
                }
            if (errors == null)
                return;
            if (1 < errors.Count)
                throw new AggregateException(errors);
            if (!(errors[0] is System.Threading.ThreadAbortException))  // don't rethrow ThreadAbortException: let the runtime do it instead of us
                throw PreserveStackTrace(errors[0]);
        }

        public static SequenceDisposerStruct DisposerStructForAll(System.Collections.IEnumerable p_seq)
        {
            return new SequenceDisposerStruct { m_seq = p_seq };
        }
        // much like System.Reactive.Disposables.CompositeDisposable, which is not part of the .NET framework
        public struct SequenceDisposerStruct : IDisposable
        {
            internal System.Collections.IEnumerable m_seq;
            public void Dispose() { DisposeAll(Utils.Exchange(ref m_seq, null)); }
        }
        /// <summary> Creates a finalizable IDisposable object: its dtor calls Dispose() when garbage collected </summary>
        public static IDisposable DisposerForAll(System.Collections.IEnumerable p_seq)
        {
            return new SequenceDisposerWithDtor { m_seq = p_seq };
        }
        private class SequenceDisposerWithDtor : DisposablePattern
        {
            internal System.Collections.IEnumerable m_seq;
            protected override void Dispose(bool p_notFromFinalize) 
            {
                DisposeAll(Utils.Exchange(ref m_seq, null));
            }
        }

        /// <summary> Returns the index of the first non-null (and non-DBNull)
        /// item in p_list, or -1 if all elements are null (or p_list is empty). </summary>
        public static int FirstNonNull<T>(IList<T> p_list)
        {
            using (var it = p_list.GetEnumerator())
                return NextNonNull(it);
        }

        /// <summary> Verifies that the current implementation of 
        /// System.Collections.Generic.Dictionary&lt;&gt; preserves 
        /// the order of addition while nothing is removed from it.
        /// This method should be called from every method (both
        /// in Debug and Release) that exploits this undocumented 
        /// feature of the .NET Framework.
        /// </summary><exception cref="PlatformNotSupportedException">
        /// if Dictionary&lt;&gt; does not preserve the order of
        /// addition and p_throwException is true. </exception>
        public static bool AssertDictionaryPreservesOrder(bool p_throwException)
        {
            if (g_doesDictionaryPreserveOrder.HasValue)
                return g_doesDictionaryPreserveOrder.Value;
            var tmp = new Dictionary<int, bool>();
            int n = 1000;
            while (--n >= 0)
                tmp[n] = true;
            n = 1000;
            bool ok = true;
            foreach (KeyValuePair<int, bool> kv in tmp)
                if (kv.Key != --n)
                {
                    ok = false;
                    break;
                }
            g_doesDictionaryPreserveOrder = ok;
            if (!ok && p_throwException)
                throw new PlatformNotSupportedException("Unexpected implementation of " 
                    + typeof(Dictionary<,>).FullName);
            return ok;
        }
        private static bool? g_doesDictionaryPreserveOrder; // = null;


        /// <summary> p_intArray[] may be null. Note: this method is equivalent to 
        /// Array.IndexOf&lt;int&gt;() but faster because it does not use EqualityComparer&lt;int&gt;.Default </summary>
        public static int IndexOf(this int[] p_intArray, int p_value)
        {
            if (p_intArray != null)
                for (int i = 0; i < p_intArray.Length; ++i)
                    if (p_intArray[i] == p_value)
                        return i;
            return -1;
        }
        /// <summary> p_intArray[] may be null </summary>
        public static bool Contains(this int[] p_intArray, int p_value)
        {
            return IndexOf(p_intArray, p_value) >= 0;
        }

        public static int IndexOf<T>(T obj, params T[] p_args)
        {
            if (p_args == null)
                return -1;
            EqualityComparer<T> cmp = EqualityComparer<T>.Default;
            for (int i = 0; i < p_args.Length; ++i)
                if (cmp.Equals(obj, p_args[i]))
                    return i;
            return -1;
        }

        /// <summary> Usage: if (Utils.Is(p_expr).EqualTo(".zip").Or(".odt").Or(".ods")) ... <para>
        /// or: if (Utils.Is(p_expr).Containing("substr1").Or("substr2")) ... </para>
        /// Allows comparing p_expr with multiple values, without evaluating p_expr multiple
        /// times. The EqualTo() operation does not involve any allocation (the Containing() does). </summary>
        public static IsStruct<T> Is<T>(T p_expr, IEqualityComparer<T> p_cmp = null)
        {
            return new IsStruct<T>(p_expr, p_cmp);
        }
        public struct IsStruct<T>
        {
            IEqualityComparer<T> m_comparer;
            T m_operand;
            bool m_result;
            Func<IsStruct<T>, object, bool> m_operation;
            public IsStruct(T p_arg, IEqualityComparer<T> p_cmp = null)
            {
                m_operand = p_arg;
                m_comparer = p_cmp ?? EqualityComparer<T>.Default;
                m_operation = null;
                m_result = false;
            }
            public IsStruct<T> EqualTo(T p_arg) { m_result = m_comparer.Equals(m_operand, p_arg); return this; }
            public IsStruct<T> Or(T p_arg)      { m_result = m_result || Predicate(p_arg); return this; }
            public IsStruct<T> And(T p_arg)     { m_result = m_result && Predicate(p_arg); return this; }
            public IsStruct<T> Containing(string p_pattern)
            {
                string s = (m_operand == null) ? String.Empty : m_operand.ToString();
                m_result = Predicate(p_pattern, (@this, ptn) => ptn != null && s.Contains(ptn.ToString()));
                return this;
            }
            bool Predicate<A>(A p_arg, Func<IsStruct<T>, object, bool> p_op = null)
            {
                if (m_operation == null && p_op == null)
                    return m_comparer.Equals(m_operand, __refvalue(__makeref(p_arg), T));   // non-boxing conversion A->T, throws if A != T
                if (p_op != null)
                    m_operation = p_op;
                return m_operation(this, p_arg);
            }
            public static implicit operator bool(IsStruct<T> p_this) { return p_this.m_result; }
        }

        public static int IndexOf<T>(this IEnumerable<T> p_seq, Predicate<T> p_predicate)
        {
            int i = 0;
            foreach (T item in p_seq.EmptyIfNull())
                if (p_predicate(item))
                    return i;
                else
                    ++i;
            return -1;
        }

        public static IEnumerable<int> IndicesOf<T>(this IEnumerable<T> p_seq, Predicate<T> p_predicate)
        {
            int i = 0;
            foreach (T item in p_seq.EmptyIfNull())
            {
                if (p_predicate(item))
                    yield return i;
                i += 1;
            }
        }

        /// <summary> Returns items of p_filteredInput (which is comprised
        /// of zero or more elements of p_input in the original order) along
        /// with their zero-based index in p_input </summary>
        public static IEnumerable<KeyValuePair<T, int>> FindIndices<T>(IEnumerable<T> p_input,
            IEnumerable<T> p_filteredInput)
        {
            EqualityComparer<T> eq = EqualityComparer<T>.Default;
            bool ok = true;
            using (var it1 = p_input.GetEnumerator())
                if (it1.MoveNext())
                    using (var it2 = p_filteredInput.GetEnumerator())  
                        for (int i = 0; it2.MoveNext(); ++i)
                        {
                            for (; ok && !eq.Equals(it1.Current, it2.Current); ++i)
                                ok = it1.MoveNext();
                            if (!ok)
                                throw new InvalidOperationException("p_input is over before finding " + it2.Current);
                            yield return new KeyValuePair<T, int>(it2.Current, i);
                            ok = it1.MoveNext();
                        }
        }

        public static void SetSize<T>(IList<T> p_list, int p_size) where T : new()
        {
            int n = p_list.Count;
            if (p_size < 0)
                p_size = 0;
            while (p_size < n)
                p_list.RemoveAt(--n);
            while (++n <= p_size)
                p_list.Add(new T());
        }

        public static void FastRemoveAt<T>(this IList<T> p_list, int p_atIdx)
        {
            int n = p_list.Count;
            if (unchecked((uint)p_atIdx < (uint)n))
            {
                if (p_atIdx < --n)
                    p_list[p_atIdx] = p_list[n];
                p_list.RemoveAt(n);
            }
        }

        /// <summary> Preserves the order of items in p_list[] </summary>
        public static void RemoveWhere<T>(this IList<T> p_list, Predicate<T> p_cond)
        {
            if (p_cond == null)
                throw new ArgumentNullException();
            if (p_list == null)
                return;
            int dst = 0, n = p_list.Count;
            for (int src = 0; src < n; ++src)
            {
                T item = p_list[src];
                if (!p_cond(item))  // = if (!remove)
                {
                    if (dst != src)
                        p_list[dst] = item;
                    ++dst;
                }
            }
            if (n <= dst)
                return;
            List<T> list = p_list as List<T>;
            if (list != null)
                list.RemoveRange(dst, n - dst);
            else while (n > dst)
                p_list.RemoveAt(--n);
        }

        public static T[] MakeArray<T>(params T[] p_args)
        {
            return p_args;
        }

        public static IEnumerable<KeyValuePair<T1, T2>> CrossProduct<T1, T2>(
            IEnumerable<T1> p_coll1, IEnumerable<T2> p_coll2)
        {
            foreach (T1 a in p_coll1)
                foreach (T2 b in p_coll2)
                    yield return new KeyValuePair<T1, T2>(a, b);
        }

        //public static IEnumerable<Rec<T1, T2>> CrossProductRec<T1, T2>(
        //    IEnumerable<T1> p_coll1, IEnumerable<T2> p_coll2)
        //{
        //    foreach (T1 a in p_coll1)
        //        foreach (T2 b in p_coll2)
        //            yield return new Rec<T1, T2>(a, b);
        //}

        public static IEnumerable<KeyValuePair<T1, T2>> MakePairs<T1, T2>(
            IEnumerable<T1> p_coll1, T2 p_value)
        {
            int n;
            var list = p_coll1 as IList<T1>;
            if (list != null)                               // Return IList<> if possible
                return new SelectListClass<T1, KeyValuePair<T1, T2>, MakePairsHelper<T1, T2>>(
                    list, new MakePairsHelper<T1, T2>(p_value));
            else if (0 < (n = Utils.TryGetCount(p_coll1)))  // Return ICollection<> if possible
                return new MinimalCollection<KeyValuePair<T1, T2>>(
                    MakePairsHelper<T1, T2>.Enumerate(p_coll1, p_value), n);
            else if (n == 0)
                return Enumerable.Empty<KeyValuePair<T1, T2>>();
            else
                return MakePairsHelper<T1, T2>.Enumerate(p_coll1, p_value);
        }
        struct MakePairsHelper<T1, T2> : IListItemSelector<T1, KeyValuePair<T1, T2>>
        {
            readonly T2 m_t2;
            internal MakePairsHelper(T2 p_t2) { m_t2 = p_t2; }
            KeyValuePair<T1, T2> IListItemSelector<T1, KeyValuePair<T1, T2>>.GetAt(int p_index, T1 p_item)
            {
                return new KeyValuePair<T1, T2>(p_item, m_t2);
            }
            internal static IEnumerable<KeyValuePair<T1, T2>> Enumerate(
                IEnumerable<T1> p_coll1, T2 p_value)
            {
                foreach (T1 a in p_coll1)
                    yield return new KeyValuePair<T1, T2>(a, p_value);
            }
        }

        public static IEnumerable<KeyValuePair<T1, T2>> MakePairs1<T1, T2>(T1 p_t1, IEnumerable<T2> p_seq)
        {
            foreach (T2 t2 in p_seq.EmptyIfNull())
                yield return new KeyValuePair<T1, T2>(p_t1, t2);
        }

        public static IEnumerable<KeyValuePair<T1, T2>> MakePairs2<T1, T2>(
            IEnumerable<T1> p_seq1, IEnumerable<T2> p_seq2)
        {
            using (var it = p_seq1.GetEnumerator())
                if (it.MoveNext())
                    foreach (T2 b in p_seq2)
                    {
                        yield return new KeyValuePair<T1, T2>(it.Current, b);
                        if (!it.MoveNext())
                            break;
                    }
        }

        /// <summary> p_settings[] should contain even number of items (key,value pairs),
        /// or a single argument of type IEnumerable&lt;KeyValuePair&lt;?,?&gt;&gt;.
        /// When T is 'string' or 'object', the '?' types can be anything, otherwise
        /// InvalidCastException may occur.
        /// When T is 'string', p_settings[0] may be a NameValueCollection, too. </summary>
        public static IEnumerable<KeyValuePair<string, T>> MakePairs<T>(params object[] p_settings)
        {
            if (p_settings == null)
                return null;
            if (p_settings.Length == 1 && p_settings[0] != null)
            {
                var result = p_settings[0] as IEnumerable<KeyValuePair<string, T>>;
                if (result != null)
                    return result;
                System.Collections.Specialized.NameValueCollection nvc;
                if (typeof(T) == typeof(string) && Utils.CanBe(p_settings[0], out nvc))
                    return Enumerable.Range(0, nvc.Count).Select(i => 
                        new KeyValuePair<string, T>(nvc.GetKey(i), (T)(object)nvc.Get(i)));

                foreach (Type enu in Utils.GetGenericTypeArgs(p_settings[0].GetType(), typeof(IEnumerable<>)))
                {
                    Type[] actual = Utils.GetGenericTypeArgs(enu, typeof(KeyValuePair<,>));
                    if (actual.Length == 2)
                    {
                        Array.Resize(ref actual, 3);
                        actual[2] = typeof(T);
                        Func<object, IEnumerable<KeyValuePair<string, T>>> converter = TemplateForMakePairs<int, int, T>;
                        Utils.ReplaceGenericParameters(converter, out converter, actual);
                        return converter(p_settings[0]);
                    }
                }
            }
            bool isString = (typeof(T) == typeof(string));
            return from i in Enumerable.Range(0, p_settings.Length / 2)
                   where p_settings[2*i] != null
                   select new KeyValuePair<string, T>(p_settings[2*i].ToString(), 
                       isString ? (T)(object)p_settings[2*i+1].ToString() : (T)(object)p_settings[2*i+1]);
        }

        static IEnumerable<KeyValuePair<string, T>> TemplateForMakePairs<K, V, T>(object p_seq)
        {
            bool isString = (typeof(T) == typeof(string));
            foreach (var kv in (p_seq as IEnumerable<KeyValuePair<K, V>>).EmptyIfNull())
                if (kv.Key != null)
                {
                    V v = kv.Value;
                    T t = isString ? (T)(object)v.ToString() : __refvalue(__makeref(v), T);
                    yield return new KeyValuePair<string, T>(kv.Key.ToString(), t);
                }
        }

        public static KeyValuePair<K, V> MakePair<K, V>(K p_key, V p_value)
        {
            return new KeyValuePair<K, V>(p_key, p_value);
        }

        public static IEnumerable<K> GetKeys<K, V>(this IEnumerable<KeyValuePair<K, V>> p_seq)
        {
            foreach (var kv in p_seq.EmptyIfNull())
                yield return kv.Key;
        }

        /// <summary> Extracts the Key property of every item provided that V is
        /// supported by KeyExtractor&lt;V,K&gt;: V is an IKeyInValue&lt;K2&gt;
        /// (where K2 is a K) or V is a KeyValuePair&lt;K2,?&gt; or
        /// IGrouping&lt;K2,?&gt;, or V is a K.
        /// Returns plain IEnumerable for IList/ICollection input, too. </summary>
        public static IEnumerable<K> GetKeysEx<V, K>(this IEnumerable<V> p_seq)
        {
            var keyExtractor = KeyExtractor<V, K>.Default;
            foreach (V v in p_seq.EmptyIfNull())
                yield return keyExtractor.GetKey(v);
        }

        public static IOrderedEnumerable<KeyValuePair<K, V>> OrderByKeys<K, V>(
            this IEnumerable<KeyValuePair<K, V>> p_seq)
        {
            return p_seq.EmptyIfNull().OrderBy(kv => kv.Key);
        }

        public static IEnumerable<KeyValuePair<K, V>> WhereKey<K, V>(this
            IEnumerable<KeyValuePair<K, V>> p_seq, Func<K, bool> p_condition)
        {
            foreach (var kv in p_seq.EmptyIfNull())
                if (p_condition(kv.Key))
                    yield return kv;
        }

        public static void SetKeyAt<TKey, TValue>(IList<KeyValuePair<TKey, TValue>> p_list, int p_idx, TKey p_key)
        {
            p_list[p_idx] = new KeyValuePair<TKey, TValue>(p_key, p_list[p_idx].Value);
        }
        public static void SetValueAt<TKey, TValue>(IList<KeyValuePair<TKey, TValue>> p_list, int p_idx, TValue p_value)
        {
            p_list[p_idx] = new KeyValuePair<TKey, TValue>(p_list[p_idx].Key, p_value);
        }

        /// <summary> Returns a new T[] by inserting objects of T or collections of T 
        /// into p_array starting at p_at. </summary>
        public static T[] Insert<T>(T[] p_array, int p_at, params object[] p_values)
        {
            int n = (p_array == null) ? 0 : p_array.Length;
            int more = 0;
            ICollection<T> coll;
            if (p_values != null)
                foreach (object arg in p_values)
                    more += CanBe(arg, out coll) ? coll.Count : 1;

            if (more == 0)
                return p_array ?? (T[])Enumerable.Empty<T>();

            T[] result = new T[n + more], array;
            if (n > 0)
            {
                Array.Copy(p_array, 0, result, 0, p_at);
                Array.Copy(p_array, p_at, result, p_at+more, n-p_at);
            }
            foreach (object arg in p_values)
            {
                if (CanBe(arg, out array))
                {
                    Array.Copy(array, 0, result, p_at, array.Length);
                    p_at += array.Length;
                }
                else if (CanBe(arg, out coll))
                {
                    coll.CopyTo(result, p_at);
                    p_at += coll.Count;
                }
                else
                {
                    result[p_at++] = (T)arg;
                }
            }
            return result;
        }

        /// <summary> Returns a new T[] by concatenating p_array with objects of T 
        /// or collections of T </summary>
        public static T[] AppendArray<T>(T[] p_array, params object[] p_values)
        {
            return Insert(p_array, (p_array == null) ? 0 : p_array.Length, (object[])p_values);
        }
        public static T[] AppendArray<T>(ref T[] p_array, T p_value)
        {
            if (p_array == null)
                p_array = new T[] { p_value };
            else
            {
                var tmp = new T[p_array.Length + 1];
                Array.Copy(p_array, 0, tmp, 0, p_array.Length);
                tmp[p_array.Length] = p_value;
                p_array = tmp;
            }
            return p_array;
        }
        public static void AppendArray<T>(ref T[] p_array, T p_value1, T p_value2)
        {
            if (p_array == null)
                p_array = new T[] { p_value1, p_value2 };
            else
            {
                var tmp = new T[p_array.Length + 2];
                Array.Copy(p_array, 0, tmp, 0, p_array.Length);
                tmp[p_array.Length] = p_value1;
                tmp[p_array.Length + 1] = p_value2;
                p_array = tmp;
            }
        }
        public static C Append<T, C>(C p_dest, params T[] p_src) where C : ICollection<T>
        {
            return Append<T, C>(p_dest, (IEnumerable<T>)p_src);
        }

        public static C Append<T, C>(C p_dest, IEnumerable<T> p_src) where C : ICollection<T>
        {
            foreach (T item in p_src)
                p_dest.Add(item);
            return p_dest;
        }

        public static IDictionary<K1, V1> AddRange<K1, V1, K2, V2>(this IDictionary<K1, V1> p_dest, 
                                                                   IEnumerable<KeyValuePair<K2, V2>> p_src)
            where K2 : K1
            where V2 : V1
        {
            if (p_src != null)
                foreach (KeyValuePair<K2, V2> kv in p_src)
                    p_dest[kv.Key] = kv.Value;
            return p_dest;
        }

        public static ICollection<T> AddRange<T>(this ICollection<T> p_collection, IEnumerable<T> p_values)
        {
            var list = p_collection as List<T>;
            if (list != null && p_values != null)
                list.AddRange(p_values);
            else foreach (T value in p_values.EmptyIfNull())
                p_collection.Add(value);
            return p_collection;
        }

        public static void AppendDict(System.Collections.IDictionary p_dest, System.Collections.IDictionary p_src)
        {
            if (p_src == null)
                return;
            System.Collections.IDictionaryEnumerator it = p_src.GetEnumerator();
            using (it as IDisposable)   // IDictionaryEnumerator is not IDisposable, but the actual implementation may be
                while (it.MoveNext())
                    p_dest[it.Key] = it.Value;
        }

        /// <summary> Consider using Array.Clear() instead of this method. </summary>
        public static C Fill<C, T>(C p_list, T p_value, int p_count = int.MaxValue) where C : IList<T>
        {
            for (int i = 0, n = Math.Min(p_count, p_list.Count); i < n; ++i)
                p_list[i] = p_value;
            return p_list;
        }

        /// <summary> Sets p_ints[p_startIdx+i] := p_startValue+i*p_step for every i in [0, p_count) </summary>
        public static int[] FillIntegers(int[] p_ints, int p_startIdx, int p_count, int p_startValue, int p_step)
        {
            const int Max = 10240/4;    // 10K is devoted for the following speed-up
            if (p_step == 1 && unchecked((uint)p_startValue < (uint)(Max-16)))
            {
                // This speed-up uses BlockCopy() instead of setting every array item one by one
                int[] template = g_templateNrSeq;   // the template to copy from (contains 0,1,2,...,Max-1)
                if (template == null)
                {
                    template = new int[Max];
                    for (int i = Max; --i >= 0; )
                        template[i] = i;
                    System.Threading.Thread.MemoryBarrier();
                    g_templateNrSeq = template;
                }
                p_step = Math.Min(Max - p_startValue, p_count);
                Buffer.BlockCopy(g_templateNrSeq, p_startValue << 2, 
                    p_ints, p_startIdx << 2, p_step << 2);
                if (p_step == p_count)
                    return p_ints;
                p_startValue += p_step;
                p_startIdx += p_step;
                p_count -= p_step;
                p_step = 1;
            }
            for (; --p_count >= 0; p_startValue += p_step)
                p_ints[p_startIdx++] = p_startValue;
            return p_ints;
        }
        static int[] g_templateNrSeq;


        // Use Utils.Factory<T>(p_count).ToArray() instead
        //public static T[] ArrayFactory<T>(int p_count) where T : new()
        //{
        //    Utils.Factory<T>(p_count).ToArray()
        //    T[] result = new T[p_count];
        //    while (--p_count >= 0)
        //        result[p_count] = new T();
        //    return result;
        //}


        #region Replacements for the Join(), GroupJoin() standard query operators
        // Note: the compiler will prefer these methods to System.Linq.Enumerable.Join()
        // when p_inner is IDictionary<> or ILookup<>, because the type of p_inner
        // is more specific in these methods.

        /// <summary> A replacement for the Join() standard query operator for cases
        /// when p_inner is an IDictionary and p_innerKeySelector selects the Key 
        /// from its argument. Note that in other cases (when p_innerKeySelector
        /// selects something other) this method involves System.Linq.Expressions.Compile() 
        /// and is therefore quite slow. You can avoid calling this method by casting
        /// the p_inner parameter to type IEnumerable&lt;KeyValuePair&lt;TKey, TInner&gt;&gt;.
        /// </summary>
        public static IEnumerable<TResult> Join<TOuter, TInnerValue, TKey, TResult>(
            this IEnumerable<TOuter> p_outer,
            IDictionary<TKey, TInnerValue> p_inner,
            Func<TOuter, TKey> p_outerKeySelector,
            Expression<Func<KeyValuePair<TKey, TInnerValue>, TKey>> p_innerKeySelector,
            Func<TOuter, KeyValuePair<TKey, TInnerValue>, TResult> p_resultSelector)
        {
            if (IsDictionaryKeySelector(p_innerKeySelector))
                return DictionaryJoin(p_outer, p_inner, p_outerKeySelector, p_resultSelector);
            return Enumerable.Join(p_outer, p_inner, p_outerKeySelector, 
                p_innerKeySelector.Compile(), p_resultSelector);
        }

        /// <summary> A replacement for the Join() standard query operator for cases
        /// when p_inner is an ILookup and p_innerKeySelector selects the Key 
        /// from its argument. Note that in other cases (when p_innerKeySelector
        /// selects something other) this method involves System.Linq.Expressions.Compile() 
        /// and is therefore quite slow. You can avoid calling this method by casting
        /// the p_inner parameter to type IEnumerable&lt;KeyValuePair&lt;TKey, TInner&gt;&gt;.
        /// </summary>
        public static IEnumerable<TResult> Join<TOuter, TInnerValue, TKey, TResult>(
            this IEnumerable<TOuter> p_outer,
            ILookup<TKey, TInnerValue> p_inner,
            Func<TOuter, TKey> p_outerKeySelector,
            Expression<Func<IGrouping<TKey, TInnerValue>, TKey>> p_innerKeySelector,
            Func<TOuter, IGrouping<TKey, TInnerValue>, TResult> p_resultSelector)
        {
            if (IsLookupKeySelector(p_innerKeySelector))
                return LookupJoin(p_outer, p_inner, p_outerKeySelector, p_resultSelector);
            return Enumerable.Join(p_outer, p_inner, p_outerKeySelector, 
                p_innerKeySelector.Compile(), p_resultSelector);
        }

        /// <summary> A replacement for the GroupJoin() standard query operator 
        /// for cases when p_inner is an IDictionary and p_innerKeySelector selects 
        /// the Key from its argument. Note that in other cases (when p_innerKeySelector
        /// selects something other) this method involves System.Linq.Expressions.Compile() 
        /// and is therefore quite slow. You can avoid calling this method by casting
        /// the p_inner parameter to type IEnumerable&lt;KeyValuePair&lt;TKey, TInner&gt;&gt;.
        /// </summary>
        public static IEnumerable<TResult> GroupJoin<TOuter, TInnerValue, TKey, TResult>(
            this IEnumerable<TOuter> p_outer,
            IDictionary<TKey, TInnerValue> p_inner,
            Func<TOuter, TKey> p_outerKeySelector,
            Expression<Func<KeyValuePair<TKey, TInnerValue>, TKey>> p_innerKeySelector,
            Func<TOuter, IEnumerable<KeyValuePair<TKey, TInnerValue>>, TResult> p_resultSelector)
        {
            if (IsDictionaryKeySelector(p_innerKeySelector))
                return DictionaryGroupJoin(p_outer, p_inner, p_outerKeySelector, p_resultSelector);
            return Enumerable.GroupJoin(p_outer, p_inner, p_outerKeySelector, 
                p_innerKeySelector.Compile(), p_resultSelector);
        }

        public static bool IsDictionaryKeySelector<TKey, TValue>(
            Expression<Func<KeyValuePair<TKey, TValue>, TKey>> p_lambda)
        { 
            MemberExpression memberExp;
            return CanBe(p_lambda.Body, out memberExp)
                   && memberExp.Expression.Equals(p_lambda.Parameters[0])
                   && memberExp.Member.Equals(typeof(KeyValuePair<TKey, TValue>).GetProperty("Key"));
        }

        public static bool IsLookupKeySelector<TKey, TValue>(
            Expression<Func<IGrouping<TKey, TValue>, TKey>> p_lambda)
        { 
            MemberExpression memberExp;
            return CanBe(p_lambda.Body, out memberExp)
                   && memberExp.Expression.Equals(p_lambda.Parameters[0])
                   && memberExp.Member.Equals(typeof(IGrouping<TKey, TValue>).GetProperty("Key"));
        }

        public static IEnumerable<TResult> DictionaryGroupJoin<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> p_outer,
            IDictionary<TKey, TInner> p_inner,
            Func<TOuter, TKey> p_outerKeySelector,
            Func<TOuter, TInner[], TResult> p_resultSelector)
        {
            TInner[] empty = new TInner[0], single = new TInner[1];
            foreach (TOuter outerVal in p_outer)
                yield return p_resultSelector(outerVal, p_inner.TryGetValue(
                    p_outerKeySelector(outerVal), out single[0]) ? single : empty);
        }
        
        public static IEnumerable<TResult> DictionaryGroupJoin<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> p_outer,
            IDictionary<TKey, TInner> p_inner,
            Func<TOuter, TKey> p_outerKeySelector,
            Func<TOuter, IEnumerable<KeyValuePair<TKey, TInner>>, TResult> p_resultSelector)
        {
            var empty  = new KeyValuePair<TKey, TInner>[0];
            var single = new KeyValuePair<TKey, TInner>[1];
            TKey key;
            TInner innerValue;
            foreach (TOuter outerVal in p_outer)
                if (p_inner.TryGetValue(key = p_outerKeySelector(outerVal), out innerValue))
                {
                    single[0] = new KeyValuePair<TKey, TInner>(key, innerValue);
                    yield return p_resultSelector(outerVal, single);
                }
                else
                    yield return p_resultSelector(outerVal, empty);
        }
        
        public static IEnumerable<TResult> DictionaryJoin<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> p_outer,
            IDictionary<TKey, TInner> p_inner,
            Func<TOuter, TKey> p_outerKeySelector,
            Func<TOuter, TInner, TResult> p_resultSelector)
        {
            TInner innerValue;
            foreach (TOuter outerVal in p_outer)
                if (p_inner.TryGetValue(p_outerKeySelector(outerVal), out innerValue))
                    yield return p_resultSelector(outerVal, innerValue);
        }

        public static IEnumerable<TResult> DictionaryJoin<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> p_outer,
            IDictionary<TKey, TInner> p_inner,
            Func<TOuter, TKey> p_outerKeySelector,
            Func<TOuter, KeyValuePair<TKey, TInner>, TResult> p_resultSelector)
        {
            TInner innerValue;
            TKey key;
            foreach (TOuter outerVal in p_outer)
                if (p_inner.TryGetValue(key = p_outerKeySelector(outerVal), out innerValue))
                    yield return p_resultSelector(outerVal, new KeyValuePair<TKey, TInner>(key, innerValue));
        }

        public static IEnumerable<TResult> LookupJoin<TOuter, TInner, TKey, TResult>(
            IEnumerable<TOuter> p_outer,
            ILookup<TKey, TInner> p_inner,
            Func<TOuter, TKey> p_outerKeySelector,
            Func<TOuter, IGrouping<TKey, TInner>, TResult> p_resultSelector)
        {
            foreach (TOuter outerVal in p_outer)
            {
                TKey key = p_outerKeySelector(outerVal);
                IEnumerable<TInner> values = p_inner[key];
                IGrouping<TKey, TInner> innerValue = values as IGrouping<TKey, TInner>;
                if (ReferenceEquals(innerValue, null))
                    yield return p_resultSelector(outerVal, new Grouping<TKey, TInner>(key, values));
                else
                    yield return p_resultSelector(outerVal, innerValue);
            }
        }

        public class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
        {
            public TKey Key { get; set; }
            public IEnumerable<TElement> Values { get; set; }
            public Grouping(TKey p_key) : this(p_key, Enumerable.Empty<TElement>()) {}
            public Grouping(TKey p_key, IEnumerable<TElement> p_values) { Key = p_key; Values = p_values; }
            public IEnumerator<TElement> GetEnumerator() { return Values.GetEnumerator(); }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return GetEnumerator(); }
        }

        /// <summary> Note: this method can be used with an IDictionary as p_inner
        /// by applying the .AsLookup() extension method to p_inner. </summary>
        public static IEnumerable<KeyValuePair<TKey, TValue?>> LeftJoinNullable<TKey, TValue>(
            IEnumerable<TKey> p_outer,
            ILookup<TKey, TValue> p_inner) where TValue : struct
        {
            foreach (TKey key in p_outer)
            {
                bool empty = true;
                foreach (TValue innerValue in p_inner[key])
                {
                    empty = false;
                    yield return new KeyValuePair<TKey, TValue?>(key, innerValue);
                }
                if (empty)
                    yield return new KeyValuePair<TKey, TValue?>(key, null);
            }
        }

        /// <summary> Note: this method can be used with an IDictionary as p_inner
        /// by applying the .AsLookup() extension method to p_inner. </summary>
        public static IEnumerable<KeyValuePair<TKey, TInner>> LeftJoin<TKey, TInner>(
            IEnumerable<TKey> p_outer,
            ILookup<TKey, TInner> p_inner) where TInner : class
        {
            foreach (TKey key in p_outer)
            {
                bool empty = true;
                foreach (TInner innerValue in p_inner[key])
                {
                    empty = false;
                    yield return new KeyValuePair<TKey, TInner>(key, innerValue);
                }
                if (empty)
                    yield return new KeyValuePair<TKey, TInner>(key, null);
            }
        }

        #endregion

        /// <summary> Wraps p_dict[] into an object implementing the ILookup
        /// interface efficiently. Designed to allow using .LeftJoin()
        /// with IDictionary&lt;&gt; objects </summary>
        public static ILookup<TKey, TValue> AsLookup<TKey, TValue>(this IDictionary<TKey, TValue> p_dict)
        {
            return new Dict2Lookup<TKey, TValue>(p_dict);
        }
        class Dict2Lookup<TKey, TValue> : ILookup<TKey, TValue>
        {
            readonly IDictionary<TKey, TValue> m_dictionary;

            public Dict2Lookup(IDictionary<TKey, TValue> p_dictionary)  { m_dictionary = p_dictionary; }
            public bool Contains(TKey p_key)                            { return m_dictionary.ContainsKey(p_key); }
            public int Count                                            { get { return m_dictionary.Count; } }
            public IEnumerator<IGrouping<TKey, TValue>> GetEnumerator() { return GetGroups().GetEnumerator(); }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return GetEnumerator(); }

            public IEnumerable<TValue> this[TKey p_key]
            {
                get
                {
                    TValue value;
                    if (m_dictionary.TryGetValue(p_key, out value))
                        return new Grouping<TKey, TValue>(p_key, new TValue[] { value });
                    return new Grouping<TKey, TValue>(p_key);
                }
            }

            IEnumerable<IGrouping<TKey, TValue>> GetGroups()
            {
                TValue[] single = new TValue[1];
                foreach (KeyValuePair<TKey, TValue> kv in m_dictionary)
                {
                    single[0] = kv.Value;
                    yield return new Grouping<TKey, TValue>(kv.Key, single);
                }
            }
        }

        /// <summary> Calls Utils.TryGetCount() and if it returns negative,
        /// copies p_sequence to a new FastGrowingList&lt;T&gt; and replaces
        /// p_sequence with it. Otherwise leaves p_sequence unchanged.
        /// Returns the number of items in p_sequence (never negative).<para>
        /// This method is designed to prepare for enumerating p_sequence
        /// several  times without repeatedly executing the enumerator
        /// function (or query) behind it. TryGetCount() is the simple
        /// heuristic used to detect whether there is an enumerator function
        /// or query behind p_sequence. Note that this simple heuristic
        /// can be "spoofed" easily (e.g. with an IEnumerable-based
        /// ICollection like MinimalCollection&lt;T&gt;). </para></summary>
        public static int ProduceOnce<T>(ref IEnumerable<T> p_sequence)
        {
            int n = TryGetCount(p_sequence);
            if (0 < n)
                return n;
            if (n == 0)
            {
                if (p_sequence == null)
                    p_sequence = Enumerable.Empty<T>();
                return 0;
            }
            // The following is "pipelined processing" when p_sequence is an IParallelEnumerable
            // (one thread collects the elements while others are producing them).
            var result = new FastGrowingList<T>();
            result.AddAll(p_sequence);
            p_sequence = result;
            return result.Count;
        }

        /// <summary> Attempts to extract the number of elements from p_sequence 
        /// without enumerating it. Returns -1 if failed: p_sequence is neither  
        /// ICollection, ICollection&lt;T&gt;, ILookup&lt;&gt; nor null.
        /// (ILookup is checked only if T is IGrouping&lt;,&gt;)
        /// </summary>
        public static int TryGetCount<T>(IEnumerable<T> p_sequence)
        {
            if (p_sequence == null)
                return 0;
            var c1 = p_sequence as System.Collections.ICollection;
            if (c1 != null)
                return c1.Count;
            ICollection<T> c2 = p_sequence as ICollection<T>;
            if (c2 != null)
                return c2.Count;
            if (TypeInfo<T>.Def.IsIGrouping)
            {
                Type tL = p_sequence.GetType().GetInterface(typeof(ILookup<,>).FullName);
                if (tL != null)
                    return (int)tL.GetProperty("Count").GetValue(p_sequence, null);
            }
            return -1;
        }

        public static int TryGetCount(System.Collections.IEnumerable p_sequence)
        {
            var coll = p_sequence as System.Collections.ICollection;
            return (coll != null) ? coll.Count : TryGetCount(CastTo<object>(p_sequence));
        }

        /// <summary> Returns p_sequence if it is T[] or IArrayBasedCollection&lt;T&gt;
        /// and p_forceCopy==false; otherwise returns a copy of p_sequence (exploits 
        /// ICollection&lt;T&gt;/ICollection if possible, or uses FastGrowingList
        /// as last resort). Returns an empty array if p_sequence==null </summary>
        public static T[] AsArray<T>(this IEnumerable<T> p_sequence, bool p_forceCopy = false)
        {
            if (p_sequence == null)
                return (T[])Enumerable.Empty<T>();
            T[] result = p_sequence as T[];
            if (result != null)
                return p_forceCopy ? (T[])result.Clone() : result;
            var a = p_sequence as IArrayBasedCollection<T>;
            if (a != null && (result = a.Array) != null
                && result.Length == a.Count)
                return p_forceCopy ? (T[])result.Clone() : result;

            var ct = p_sequence as ICollection<T>;
            if (ct != null)
            {
                result = new T[ct.Count];
                ct.CopyTo(result, 0);
                return result;
            }
            var c = p_sequence as System.Collections.ICollection;
            if (c != null || a != null)
            {
                result = new T[c != null ? c.Count : a.Count];
                if (c == null)
                    Array.Copy(a.Array, result, result.Length);
                else
                {
                    // Now p_sequence does not implement ICollection<T> but implements the non-generic ICollection.
                    // If it implements ICollection<X> for some X!=T, then the non-generic ICollection is likely
                    // to be about X, i.e. CopyTo(X[],int) is expected instead of CopyTo(T[],int) -- this latter
                    // would result in exception. Although this exception is handled below, avoid it if possible
                    // because it is so uncomfortable during debugging.
                    if (!IsGenericICollection(p_sequence.GetType()))
                    {
                        try
                        {
                            // "Target array type is not compatible with the type of items in the collection."
                            // This may occur when p_sequence implements IEnumerable with multiple item types,
                            // including T. In this case the non-generic ICollection.CopyTo() implementation
                            // (c.CopyTo()) may not accept T[] as target array.
                            // For example, DefaultSettings is derived from Dictionary<object,object> and
                            // implements IEnumerable<KeyValue<string,object>> in addition. The inherited
                            // CopyTo() cannot copy to a KeyValue<string,object>[] target array.
                            c.CopyTo(result, 0);
                            return result;
                        }
                        catch (ArrayTypeMismatchException) { }
                        catch (ArgumentException) { }
                    }
                    int n = 0;
                    foreach (T t in p_sequence)
                        result[n++] = t;
                    Array.Resize(ref result, n);
                }
                return result;
            }
            //return Enumerable.ToArray(p_sequence);
            return new FastGrowingList<T>().AddAll(p_sequence).ToArray();
        }

        public static FastGrowingList<T> ToFastGList<T>(this IEnumerable<T> p_sequence)
        {
            return new FastGrowingList<T>().AddAll(p_sequence);
        }

        public static T[] ToArrayFast<T>(this IEnumerable<T> p_sequence)
        {
            return AsArray(p_sequence, true);
        }

        /// <summary> Returns p_sequence if it is a List&lt;T&gt;, otherwise 
        /// returns a new List&lt;T&gt; containing a copy of p_sequence.
        /// </summary>
        public static List<T> AsList<T>(this IEnumerable<T> p_sequence)
        {
            return p_sequence as List<T> ?? new List<T>(p_sequence.EmptyIfNull());
        }

        /// <summary> Returns p_sequence if it is an IList&lt;T&gt; (e.g. T[], 
        /// List&lt;T&gt;, AbstractList&lt;T&gt;, IArrayBasedCollection&lt;T&gt;
        /// etc.), otherwise returns a new T[] containing a copy of p_sequence.
        /// </summary>
        public static IList<T> AsIList<T>(this IEnumerable<T> p_sequence)
        {
            var result = p_sequence as IList<T>;
            if (result != null || p_sequence == null)
                return result;
            var a = p_sequence as IArrayBasedCollection<T>;
            if (a != null && (result = a.Array) != null 
                && result.Count == a.Count)
                return result;
            //return new List<T>(p_sequence.EmptyIfNull());
            return p_sequence.ToArrayFast();
        }

        /// <summary> Returns p_sequence if it is an ICollection&lt;T&gt; (e.g. T[], 
        /// List&lt;T&gt;, HashSet&lt;T&gt; etc.) otherwise returns a FastGrowingList&lt;T&gt;
        /// containing a copy of p_sequence.
        /// </summary>
        public static ICollection<T> AsCollection<T>(this IEnumerable<T> p_sequence)
        {
            return p_sequence as ICollection<T> ?? new FastGrowingList<T>().AddAll(p_sequence);
        }

        /// <summary> Returns p_sequence if it is an ICollection&lt;T&gt; (e.g. T[], 
        /// List&lt;T&gt;, HashSet&lt;T&gt; etc.). Otherwise wraps p_sequence into
        /// a TypedCollection&lt;T&gt;, which will cast every element to type T if
        /// p_sequence is a non-generic ICollection, or otherwise construct a new 
        /// List&lt;T&gt; containing a copy of p_sequence.
        /// </summary>
        public static ICollection<T> AsCollection<T>(this System.Collections.IEnumerable p_sequence)
        {
            return p_sequence as ICollection<T> ?? new TypedCollection<T>(p_sequence);
        }

        /// <summary> Returns a collection with efficient implementation for Contains().
        /// Returns p_sequence itself if it is a HashSet&lt;&gt; or a Dictionary&lt;T,?&gt;.KeyCollection </summary>
        public static ICollection<T> AsSet<T>(this IEnumerable<T> p_sequence)
        {
            if (p_sequence == null)
                return null;
            Type[] dummy;
            if ((p_sequence is HashSet<T>) || IsGenericImpl(p_sequence.GetType(),
                typeof(Dictionary<int, int>.KeyCollection), out dummy, null, typeof(T)))
                return (ICollection<T>)p_sequence;
            return new HashSet<T>(p_sequence);
        }

        /// <summary> Returns a read-only IList&lt;&gt;.
        /// p_selector: the first argument is p_srcIndex (int) the second is p_srcItem (T1)</summary>
        public static IList<T2> SelectList<T1, T2>(this IList<T1> p_src, Func<int, T1, T2> p_selector)
        {
            return new SelectListClass<T1, T2, DefaultListItemSelector<T1,T2>>(p_src, p_selector);
        }
        ///// <summary> Note: this method usually raises compiler error CS0411 ("The type
        ///// arguments for method '...' cannot be inferred from the usage. Try specifying
        ///// the type arguments explicitly."). The problem is that the compiler is not
        ///// smart enough to infer T2. Explicit specification of type arguments solves it.
        ///// </summary>
        //public static IList<T2> SelectList<T1, T2, S>(this IList<T1> p_list, S p_selector)
        //    where S : IListItemSelector<T1, T2>
        //{
        //    return new SelectListClass<T1, T2, S>(p_list, p_selector);
        //}
        /// <summary> Returns a read-only IList&lt;&gt; </summary>
        public static IList<T2> SelectList<T1, T2>(this IListItemSelector<T1, T2> p_selector, IList<T1> p_list)
        {
            return new SelectListClass<T1, T2, IListItemSelector<T1, T2>>(p_list, p_selector);
        }

        public class SelectListClass<T1, T2, S> : AbstractList<T2> where S : IListItemSelector<T1, T2>
        {
            readonly IList<T1> m_source;
            readonly S m_selector;
            public SelectListClass(IList<T1> p_source, S p_selector)
            {
                m_source = p_source ?? (T1[])Enumerable.Empty<T1>();
                m_selector = p_selector;
            }
            public override int Count { get { return m_source.Count; } }
            public override T2 this[int index]
            {
                get { return m_selector.GetAt(index, m_source[index]); }
                set { throw new InvalidOperationException(); }
            }
        }

        /// <summary> Creates an ICollection&lt;T&gt; containing p_sequence. 
        /// p_count must be the number of elements in p_sequence. </summary>
        public static MinimalCollection<T> MakeCollection<T>(IEnumerable<T> p_sequence, int p_count)
        {
            return new MinimalCollection<T>(p_sequence, p_count);
        }

        /// <summary> Extracts the p_column'th item from every item (row) of p_matrix </summary>
        public static IEnumerable<T> Project<T, C>(IEnumerable<C> p_matrix, int p_column) where C : IEnumerable<T>
        {
            if (p_matrix != null && p_column >= 0)
            {
                foreach (C row in p_matrix)
                {
                    IList<T> list = row as IList<T>;
                    if (list != null)
                    {
                        if (list.Count > p_column)
                            yield return list[p_column];
                    }
                    else
                    {
                        int i = -1;
                        foreach (T t in row)
                            if (++i == p_column) { yield return t; break; }
                    }
                }
            }
        }

        /// <summary> Returns a read-only collection. Equivalent to p_coll.Select(p_projection),
        /// but preserves the Count, too. </summary>
        public static MinimalCollection<B> ProjectCollection<A, B>(ICollection<A> p_coll, Func<A, B> p_projection)
        {
            return new MinimalCollection<B>(p_coll.Select(p_projection), p_coll.Count);
        }
        /// <summary> A read-only collection that is based on an IEnumerable + count. </summary>
        public class MinimalCollection<T> : AbstractCollection<T>
        {
            readonly int m_count;
            readonly IEnumerable<T> m_sequence;
            public MinimalCollection(IEnumerable<T> p_sequence, int p_count)
            {
                m_sequence = p_sequence;
                m_count = p_count;
            }
            public override int             Count           { get { return m_count; } }
            public override IEnumerator<T>  GetEnumerator() { return m_sequence.GetEnumerator(); }
        }

        //public static ICollection<TBase> CastCollection<TDerived, TBase>(
        //    this ICollection<TDerived> p_src) where TDerived : TBase
        //{
        //    return new CastToBaseCollection<TDerived, TBase>(p_src);
        //}

        /// <summary> p_args[] may contain objects of type T or IEnumerable&lt;T&gt; </summary>
        public static IEnumerable<T> Concat<T>(params object[] p_args)
        {
            if (p_args != null)
                foreach (object arg in p_args)
                {
                    var seq = arg as IEnumerable<T>;
                    if (seq == null)
                        yield return (T)arg;
                    else foreach (T t in seq)
                        yield return t;
                }
        }

        /// <summary> Precondition: p_enumerator.Current is already valid, 
        /// and the caller is responsible for disposing p_enumerator </summary>
        public static IEnumerable<T> Continue<T>(IEnumerator<T> p_enumerator)
        {
            do
            {
                yield return p_enumerator.Current;
            } while (p_enumerator.MoveNext());
        }

/*      /// <summary> Enumerates p_iterator and disposes it at the end. This method
        /// is intended to be used in cases when the caller cannot undertake disposal 
        /// of the wrapped IEnumerator. (Iterator&lt;&gt; has a destructor which 
        /// disposes the wrapped IEnumerator even if the IEnumerable returned by this 
        /// method never gets enumerated.) </summary>
        public static IEnumerable<T> ContinueAndDispose<T>(Iterator<T> p_iterator)
        {
            using (p_iterator)
                if (p_iterator.Continue())
                    do {
                        yield return p_iterator.Current;
                    } while (p_iterator.MoveNext());
        }
*/

        /// <summary> Enumerable.Take() for IEnumerators.
        /// Precondition: it.Current is valid (it.MoveNext() has been called) </summary>
        public static IEnumerable<T> Take<T>(this IEnumerator<T> p_enumerator, uint p_count, bool[] p_hasMore = null)
        {
            bool hasMore = (p_hasMore == null || p_hasMore[0]);
            for (; hasMore && unchecked(p_count--) > 0; hasMore = p_enumerator.MoveNext())
                yield return p_enumerator.Current;
            if (p_hasMore != null)
                p_hasMore[0] = hasMore;
        }

        /// <summary> Advances p_it until the first non-null (and non-DBNull)
        /// item and returns the number of null items encountered. Returns -1
        /// if the end of p_it is reached without finding any non-null item.
        /// </summary>
        public static int NextNonNull(System.Collections.IEnumerator p_it)
        {
            int i = 0;
            for (; p_it.MoveNext(); ++i)
                if (p_it.Current != null && !(p_it.Current is DBNull))
                    return i;
            return -1;
        }

        /// <summary> Note: OfType&lt;&gt;() would do the same work but is very obscure </summary>
        public static IEnumerable<T> WhereNotNull<T>(this IEnumerable<T> p_seq)
        {
            if (p_seq == null)
                return Enumerable.Empty<T>();
            if (default(T) != null)             // T is non-nullable value type: all items are non-null
                return p_seq;
            return WhereNotNull_impl<T>(p_seq); // T is nullable or reference type
        }
        static IEnumerable<T> WhereNotNull_impl<T>(IEnumerable<T> p_seq)
        {
            foreach (T t in p_seq)
                if (t != null)
                    yield return t;
        }

        /// <summary> Consider using System.Linq.Enumerable.Repeat() instead of this method.
        /// This method produces an IList&lt;&gt; instead of just a plain IEnumerable&lt;&gt;. 
        /// </summary>
        public static RepeatClass<T> Repeat<T>(T p_value, int p_cnt)
        {
            //while (--p_cnt >= 0)
            //    yield return p_value;
            return new RepeatClass<T>(p_value, p_cnt);
        }
        public class RepeatClass<T> : AbstractList<T>
        {
            T m_value;
            int m_count;
            internal RepeatClass(T p_value, int p_count)
            {
                m_value = p_value;
                m_count = p_count;
            }
            public override int Count           { get { return m_count; } }
            public override T   this[int index] { get { return m_value; } 
                                                  set { throw new NotSupportedException(); } }
        }

        /// <summary> Returns a one-length sequence containing p_item only.<para>
        /// 'new T[] { p_item }' could be an alternative implementation,
        /// but that would cause 2 allocations on the heap: one for the array,
        /// second for the array's IEnumerator when enumerated.</para>
        /// This function uses the enumerator-function approach, which involves
        /// only 1 allocation if the result is enumerated in the same thread
        /// at most once. </summary>
        public static IEnumerable<T> Single<T>(T p_item) { yield return p_item; }

        /// <summary> As opposed to System.Linq.Enumerable.Reverse(), this method
        /// uses reduced number of reallocations: 1) does not copy at all when the
        /// input is an IList&lt;&gt;; 2) uses FastGrowingList otherwise.
        /// In contrast with List&lt;&gt;.Reverse(), this method does not
        /// modify the original p_seq. </summary>
        public static IEnumerable<T> ReverseFast<T>(this IEnumerable<T> p_seq)
        {
            IList<T> list = p_seq as IList<T>;
            return (list != null) ? new ReverseView<T>(list, true, true)
                                  : new FastGrowingList<T>().AddAll(p_seq).Reverse();
        }

        /// <summary> As opposed to System.Linq.Enumerable.Reverse(), this
        /// method produces an IList&lt;&gt; and buffers the input only when
        /// p_seq is not an IList&lt;&gt;.
        /// In contrast with List&lt;&gt;.Reverse(), this method does not
        /// modify the original p_seq (the returned IList is read-only) </summary>
        public static AbstractList<T> ReverseList<T>(this IEnumerable<T> p_seq)
        {
            return new ReverseView<T>(p_seq, p_saveCount: true, p_isReadOnly: true);
        }
        /// <summary> The returned IList is not read-only when p_readOnly==false </summary>
        public static AbstractList<T> ReverseList<T>(this IEnumerable<T> p_seq, bool p_readOnly)
        {
            return new ReverseView<T>(p_seq, true, p_readOnly);
        }
        // sealed: to speed up 'Count' in OrigIdx()
        public sealed class ReverseView<T> : AbstractList<T>
        {
            IList<T> m_original;
            bool m_readOnly;
            readonly int m_count;
            public ReverseView(IEnumerable<T> p_list, bool p_saveCount, bool p_isReadOnly = true)
            {
                m_original = p_list.AsIList();
                m_readOnly = p_isReadOnly;
                m_count    = p_saveCount ? m_original.Count : -1;
            }
            int OrigIdx(int p_idx)              { return Count - 1 - p_idx; }
            void AssertNotReadOnly()            { if (m_readOnly) throw new NotSupportedException(); }
            public override bool IsReadOnly     { get { return m_readOnly; } }
            public override int Count           { get { return (m_count < 0 ? m_original.Count : m_count); } }
            public override T   this[int index] {
                get { return m_original[OrigIdx(index)]; }
                set { AssertNotReadOnly(); m_original[OrigIdx(index)] = value; }
            }
            public override void Insert(int index, T item)
            {
                AssertNotReadOnly();
                m_original.Insert(OrigIdx(index) + 1, item);
            }
            public override void RemoveAt(int index)
            {
                AssertNotReadOnly();
                m_original.RemoveAt(OrigIdx(index));
            }
            public override void Clear()
            {
                AssertNotReadOnly();
                m_original.Clear();
            }
        }

        /// <summary> p_enumerateCollection==false prevents enumeration of non-ICollection sequences,
        /// but others are still enumerated! </summary>
        public static void ForEach(this System.Collections.IEnumerable p_sequence, bool p_enumerateCollection = false)
        {
            System.Collections.IEnumerator it;
            if (p_sequence != null && (p_enumerateCollection || TryGetCount(p_sequence) < 0))
                using ((it = p_sequence.GetEnumerator()) as IDisposable)
                    while (it.MoveNext())
                    { }
        }

        /// <summary> Fetches the first element of p_sequence and returns 
        /// true if it is available. Then replaces p_sequence to a different
        /// IEnumerable which "inserts" the fetched element back to the 
        /// beginning of the sequence. 
        /// This new IEnumerable is an Iterator object, which:
        /// - does not support multiple enumerations of the sequence 
        /// - should be disposed by the caller (or enumerated)
        /// - has a destructor which disposes it (and the enumerator of the 
        ///   original sequence). This is for the case if the caller neither 
        ///   enumerate nor dispose the new p_sequence. Note that this dtor
        ///   is a performance penalty for the whole application through the 
        ///   GC, and should be avoided by a) disposing the returned new
        ///   p_sequece; or b) casting p_sequence to Iterator and setting
        ///   its DtorBehavior property to DoNothing.
        /// </summary>
        public static bool IsEmptyLookAhead<T>(ref IEnumerable<T> p_sequence)
        {
            if (p_sequence == null)
                return true;
            IEnumerator<T> it = null;
            try
            {
                it = p_sequence.GetEnumerator();
                if (it != null && it.MoveNext())
                {
                    p_sequence = new Iterator<T>(Continue(it));
                    it = null;  // don't dispose it here
                    return false;
                }
            }
            finally
            {
                if (it != null)
                    it.Dispose();
            }
            p_sequence = Enumerable.Empty<T>();
            return true;
        }

        public static bool IsEmpty(this System.Collections.IEnumerable p_sequence)
        {
            switch (TryGetCount(p_sequence))
            {
                default: return false;
                case  0: return true;
                case -1: var it = p_sequence.GetEnumerator();
                         using (it as IDisposable)
                            return !it.MoveNext();
            }
        }

        public static bool IsEmpty<T>(this IEnumerable<T> p_sequence)
        {
            if (p_sequence == null)
                return true;
            var c = p_sequence as ICollection<T>;
            return (c != null) ? c.Count == 0 : IsEmpty((System.Collections.IEnumerable)p_sequence);
        }

        
        public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> p_sequence)
        {
            return p_sequence ?? Enumerable.Empty<T>();
        }

        /// <summary> Stops when reaching the end of either p_from or p_to[] (doesn't grow p_to[], so it may be an Array). </summary>
        public static TIList CopyTo<T, TIList>(this IEnumerable<T> p_from, TIList p_to, int p_startIdx = 0) where TIList : IList<T>
        {
            if (p_from != null)
            {
                int n = (p_to == null) ? 0 : p_to.Count;
                if (p_startIdx < n)
                    foreach (T t in p_from)
                    {
                        p_to[p_startIdx] = t;
                        if (n <= ++p_startIdx)
                            break;
                    }
            }
            return p_to;
        }

        /// <summary> Reads elements from p_seq into an internal buffer of size
        /// p_buffSize, passes the buffer to p_bufferEvent(), and returns the
        /// contents of the buffer. Repeats this until p_seq is exhausted. 
        /// Note: p_bufferEvent() may inject, remove or modify elements. 
        /// </summary>
        public static IEnumerable<T> BufferedRead<T>(IEnumerable<T> p_seq, 
            int p_buffSize, Action<IList<T>> p_bufferEvent)
        {
            if (p_buffSize < 0 || p_bufferEvent == null)
                throw new ArgumentException();
            int n = Utils.TryGetCount(p_seq);
            using (IEnumerator<T> it = p_seq.EmptyIfNull().GetEnumerator())
            {
                bool ended = !it.MoveNext();
                p_buffSize = ended ? 0 : (n < 0 ? p_buffSize : Math.Min(n, p_buffSize));
                var buff = new QuicklyClearableList<T>().EnsureCapacity(p_buffSize);
                do
                {
                    if (!ended)
                    {
                        buff.Add(it.Current);
                        ended = !it.MoveNext();
                    }
                    if (ended || p_buffSize <= buff.m_count)
                    {
                        p_bufferEvent(buff);
                        for (int i = 0; i < buff.m_count; ++i)
                            yield return buff.m_array[i];
                        buff.Clear();
                    }
                } while (!ended);
            }
        }

        public static bool SetEquals<T>(IEnumerable<T> p_a, IEnumerable<T> p_b,
            IEqualityComparer<T> p_comparer)
        {
            if (ReferenceEquals(p_a, p_b))
                return true;
            if (p_comparer == null)
                p_comparer = (p_a is HashSet<T>) ? ((HashSet<T>)p_a).Comparer
                                                 : EqualityComparer<T>.Default;
            var set = new Dictionary<T, bool>(Math.Max(TryGetCount(p_a), 0), p_comparer);
            int wasNull = 0;
            foreach (T a in p_a)
                if (a == null)
                    wasNull |= 1;
                else
                    set[a] = true;
            foreach (T b in p_b)
                if (b == null)
                    wasNull |= 2;
                else if (!set.ContainsKey(b))
                    return false;
                else
                    set[b] = false;
            return (wasNull == 0 || wasNull == 3)
                && set.Values.Where(b => b).IsEmpty();
        }

        public static bool SetEquals<T>(IEnumerable<T> p_a, params T[] p_b)
        {
            return SetEquals<T>(p_a, p_b, null);
        }

        /// <summary> Consider using Linq.Enumerable.SequenceEquals(). This function differs
        /// in that it returns 'int' (not bool) + this function checks ReferenceEquals()
        /// first + this function works when one or both arguments are null. </summary>
        public static int SequenceCompare<T>(IEnumerable<T> p_seq1, IEnumerable<T> p_seq2,
            IComparer<T> p_cmp = null)
        {
            if (p_seq1 == p_seq2)
                return 0;
            if (p_seq1 == null || p_seq2 == null)
                return System.Collections.Comparer.Default.Compare(p_seq1, p_seq2);
            IComparer<T> cmp = p_cmp ?? Comparer<T>.Default;
            using (var it1 = p_seq1.GetEnumerator())
                using (var it2 = p_seq2.GetEnumerator())
                    while (true)
                    {
                        if (it1.MoveNext())
                        {
                            if (!it2.MoveNext())
                                return 1;
                            int result = cmp.Compare(it1.Current, it2.Current);
                            if (result != 0)
                                return result;
                        }
                        else if (it2.MoveNext())
                            return -1;
                        else
                            return 0;
                    }
        }

        public static int GetHashCode(params object[] p_args)
        {
            return new CompositeHashCode(p_args);
        }

        public static int GetHashCode<T>(T p_obj)
        {
            return p_obj == null ? 0 : p_obj.GetHashCode();
        }

        ///// <summary> See also Utils.GetHashCode() </summary>
        //public static int SequenceHashCode<T>(this IEnumerable<T> p_seq)
        //{
        //    int result = 0;
        //    if (p_seq != null)
        //        foreach (T t in p_seq)
        //            result = result * 31 + (Utils.IsNull(t) ? 0 : t.GetHashCode());
        //    return result;
        //}

        ///// <summary> Consider using System.Linq.Enumerable.Max<>() instead of this method. </summary>
        //public static T FindMax<T>(IEnumerable<T> p_coll) where T : IComparable
        //{
        //    return MaxIdx(p_coll).Value;
        //}

        /// <summary> Returns both the index (zero-based) and the maximal element of p_seq.
        /// If p_cmp is null, the default comparison is used (T must be IComparable).
        /// Returns {-1, default(T)} if p_seq is empty. </summary>
        public static KeyValuePair<int, T> MaxIdx<T>(this IEnumerable<T> p_seq,
            Comparison<T> p_cmp)
        {
            if (p_cmp == null)
                p_cmp = Comparer<T>.Default.Compare;

            T maxItem = default(T);
            int maxIdx = -1, i = 0;
            foreach (T item in p_seq)
            {
                if (maxIdx < 0 || p_cmp(maxItem, item) < 0)
                {
                    maxIdx = i;
                    maxItem = item;
                }
                i += 1;
            }
            return new KeyValuePair<int, T>(maxIdx, maxItem);
        }

        /// <summary> T must be either IComparable or IComparable&lt;T&gt; </summary>
        public static T Max<T>(T p_first, params T[] p_args)
        {
            if (p_args != null)
                foreach (T t in p_args)
                    if (Comparer<T>.Default.Compare(p_first, t) < 0)
                        p_first = t;
            return p_first;
        }

        /// <summary> T must be either IComparable or IComparable&lt;T&gt; </summary>
        public static T Min<T>(T p_first, params T[] p_args)
        {
            if (p_args != null)
                foreach (T t in p_args)
                    if (Comparer<T>.Default.Compare(p_first, t) > 0)
                        p_first = t;
            return p_first;
        }

        /// <summary> T must be either IComparable or IComparable&lt;T&gt; </summary>
        public static void Max<T>(ref T p_first, params T[] p_args)
        {
            p_first = Max(p_first, p_args);
        }

        /// <summary> T must be either IComparable or IComparable&lt;T&gt; </summary>
        public static void Min<T>(ref T p_first, params T[] p_args)
        {
            p_first = Min(p_first, p_args);
        }

        /// <summary> T must be either IComparable or IComparable&lt;T&gt; </summary>
        public static T Min<T>(T p_first, T p_second)
        {
            return Comparer<T>.Default.Compare(p_first, p_second) <= 0 ? p_first : p_second;
        }

        /// <summary> T must be either IComparable or IComparable&lt;T&gt; </summary>
        public static T Max<T>(T p_first, T p_second)
        {
            return Comparer<T>.Default.Compare(p_first, p_second) >= 0 ? p_first : p_second;
        }

        public static DateTime Min(DateTime p_first, DateTime p_second)
        {
            return (p_first <= p_second) ? p_first : p_second;
        }

        /// <summary> Consider using System.Linq.Enumerable.Sum&lt;&gt;() instead of
        /// this method. This one works for nullable types + enum types + null input </summary>
        public static T Sum<T>(IEnumerable<T> p_coll)
        {
            double sum = 0;
            switch (Type.GetTypeCode(typeof(T)))
            {
                case TypeCode.Double :
                    foreach (double d in EmptyIfNull(p_coll as IEnumerable<double>))
                        sum += d;
                    break;
                case TypeCode.Int32 :
                    foreach (int i in EmptyIfNull(p_coll as IEnumerable<int>))
                        sum += i;
                    break;
                default :
                    var conv = Conversion<T, double>.Default;
                    if (TypeInfo<T>.Def.IsNullableOrRef)
                        foreach (T t in p_coll)
                            sum += conv.DefaultOnNull(t);
                    else
                        foreach (T t in p_coll)
                            sum += conv.ThrowOnNull(t);
                    break;
            }
            return Conversion<double, T>.Default.ThrowOnNull(sum);
        }

        /// <summary> Returns the first one of every repeated equal elements
        /// (in terms of p_eq(prev,current)). p_seq should be sorted so that 
        /// equal elements follow each other.
        /// If p_eq == null, EqualityComparer&lt;T&gt;.Default.Equals() will be used.
        /// Note: p_eq(null,*) should not crash if p_seq may contain nulls.
        /// </summary>
        /// <remarks> This method is more efficient than System.Linq.Enumerable.Distinct()
        /// because that one employs a HashSet. Use that when p_seq isn't (and needn't be)
        /// sorted. </remarks>
        public static IEnumerable<T> MakeUniqueEqCmp<T>(this IEnumerable<T> p_seq, IEqualityComparer<T> p_eq = null)
        {
            bool first = true;
            T last = default(T);
            IEqualityComparer<T> eq = p_eq ?? EqualityComparer<T>.Default;
            foreach (T t in p_seq.EmptyIfNull())
            {
                if (first)
                {
                    last = t;
                    first = false;
                }
                else if (!eq.Equals(last, t))
                {
                    yield return last;
                    last = t;
                }
            }
            if (!first)
                yield return last;
        }
        public static IEnumerable<T> MakeUniqueEq<T>(this IEnumerable<T> p_seq, Func<T, T, bool> p_eq)
        {
            return MakeUniqueEqCmp<T>(p_seq, new EqCmp<T>(p_eq));
        }
        public static IEnumerable<T> MakeUniqueCmp<T>(this IEnumerable<T> p_seq, IComparer<T> p_cmp = null)
        {
            return MakeUniqueEqCmp<T>(p_seq, new EqCmp<T>(p_cmp));
        }
        public class EqCmp<T> : IEqualityComparer<T>
        {
            readonly IComparer<T> m_comparer;
            readonly Func<T, T, bool> m_delegate;
            public EqCmp(IComparer<T> p_cmp)            { m_comparer = p_cmp ?? Comparer<T>.Default; }
            public EqCmp(Func<T, T, bool> p_delegate)   { m_delegate = p_delegate; }
            public bool Equals(T x, T y)
            {
 	            return (m_comparer != null) ? m_comparer.Compare(x, y) == 0 : m_delegate(x, y);
            }
            public int GetHashCode(T obj) { return obj == null ? 0 : obj.GetHashCode(); }
        }

        /// <summary> Sorts the sequence in-place if it is a T[]/List&lt;T&gt;/IArrayBasedCollection&lt;T&gt;(*),
        /// otherwise returns a sorted copy of the sequence. Never returns null (instead the empty array).
        /// (*): IArrayBasedCollection&lt;T&gt; is exploited only if Count == Array.Length </summary>
        public static IList<T> Sort<T>(this IEnumerable<T> p_seq, IComparer<T> p_comparer = null, bool p_stable = false)
        {
            return SortImpl<T>(p_seq, p_stable, p_comparer);
        }

        public static IList<T> Sort<T>(this IEnumerable<T> p_seq, Comparison<T> p_comparison, bool p_stable = false)
        {
            return SortImpl<T>(p_seq, p_stable, p_comparison);
        }

        static IList<T> SortImpl<T>(this IEnumerable<T> p_seq, bool p_stable, object p_cmp)
        {
            var list = p_stable ? null : p_seq as List<T>;
            IComparer<T> cmp;
            if (list != null)
            {
                if (p_cmp == null)
                    list.Sort();
                else if (null != (cmp = p_cmp as IComparer<T>))
                    list.Sort(cmp);
                else
                    list.Sort((Comparison<T>)p_cmp);
                return list;
            }
            T[] result = p_seq.AsArray();   // 'result' is never null
            if (p_stable)
                new StableSort<T>(result, p_cmp);
            else if (p_cmp == null)
                Array.Sort(result);
            else if (null != (cmp = p_cmp as IComparer<T>))
                Array.Sort(result, cmp);
            else
                Array.Sort(result, (Comparison<T>)p_cmp);
            return result;
        }
        class StableSort<T> : IComparer<int>
        {
            readonly T[] m_array;
            readonly int[] m_helper;
            readonly IComparer<T> m_comparer;
            readonly Comparison<T> m_comparison;
            public StableSort(T[] p_array, object p_cmp)
            {
                m_array = p_array;
                m_helper = FillIntegers(new int[m_array.Length], 0, m_array.Length, 0, 1);
                m_comparison = p_cmp as Comparison<T>;
                if (m_comparison == null)
                    m_comparer = (p_cmp as IComparer<T>) ?? Comparer<T>.Default;
                Array.Sort(m_helper, m_array, this);
            }
            int IComparer<int>.Compare(int x, int y)
            {
                int result = (m_comparison != null) ? m_comparison(m_array[x], m_array[y])
                                                    : m_comparer.Compare(m_array[x], m_array[y]);
                // "(result == 0) ? x - y : result" without branch:
                return ~((result | -result) >> 31) & (x - y);
            }
        }

        /// <summary> Moves p_node between two lists or within a list. If p_beforeThis is null, 
        /// p_node is moved to the beginning of its current list. </summary>
        public static void MoveToBefore<T>(LinkedListNode<T> p_beforeThis, LinkedListNode<T> p_node)
        {
            LinkedList<T> list = p_node.List;
            if (p_beforeThis == null)
            {
                Utils.DebugAssert(list != null);
                if (!ReferenceEquals(list.First, p_node))
                {
                    list.Remove(p_node);
                    list.AddFirst(p_node);
                }
            }
            else if (!ReferenceEquals(p_beforeThis, p_node))
            {
                if (list != null)
                    list.Remove(p_node);
                p_beforeThis.List.AddBefore(p_beforeThis, p_node);
            }
        }

        /// <summary> Moves a node between two lists or within a list. If p_afterThis is null, 
        /// p_node is moved to the end of its current list. </summary>
        public static void MoveToAfter<T>(LinkedListNode<T> p_afterThis, LinkedListNode<T> p_node)
        {
            LinkedList<T> list = p_node.List;
            if (p_afterThis == null)
            {
                Utils.DebugAssert(list != null);
                if (!ReferenceEquals(list.Last, p_node))
                {
                    list.Remove(p_node);
                    list.AddLast(p_node);
                }
            }
            else if (!ReferenceEquals(p_afterThis, p_node))
            {
                if (list != null)
                    list.Remove(p_node);
                p_afterThis.List.AddAfter(p_afterThis, p_node);
            }
        }

        public static void MoveToBeginning<T>(LinkedList<T> p_list, LinkedListNode<T> p_node)
        {
            var list = p_node.List;
            if (list != null)
                list.Remove(p_node);
            p_list.AddFirst(p_node);
        }

        public static void MoveToEnd<T>(LinkedList<T> p_list, LinkedListNode<T> p_node)
        {
            var list = p_node.List;
            if (list != null)
                list.Remove(p_node);
            p_list.AddLast(p_node);
        }


        /// <summary> Removes p_node from its current list. Returns the removed node. 
        /// p_node advances to the next node (become null if it was already the last).
        /// Either p_node and p_node.List may be null. </summary>
        public static LinkedListNode<T> RemoveAndAdvance<T>(ref LinkedListNode<T> p_node)
        {
            LinkedListNode<T> result = p_node;
            if (p_node != null)
            {
                LinkedList<T> list = p_node.List;
                p_node = p_node.Next;
                if (list != null)
                    list.Remove(result);
            }
            return result;
        }

        #region SubList<T>
        /// <summary> Read-only range of p_list: [p_first, end]. Dynamic change of 'end' is reflected.</summary>
        public static SubListStruct<T> SubList<T>(IList<T> p_list, int p_first)
        {
            return new SubListStruct<T>(p_list, p_first, int.MaxValue, true);
        }
        /// <summary> Range of p_list: [p_first, p_last] inclusive </summary>
        public static SubListStruct<T> SubList<T>(IList<T> p_list, int p_first, int p_last,
            bool p_readOnly)
        {
            return new SubListStruct<T>(p_list, p_first, p_last, p_readOnly);
        }
        [DebuggerTypeProxy("System.Collections.Generic.Mscorlib_CollectionDebugView`1, mscorlib")]
        public struct SubListStruct<T> : IList<T>
        {
            readonly IList<T> m_list;
            readonly bool m_readOnly;
            int m_first, m_last;
            internal SubListStruct(IList<T> p_list, int p_first, int p_last, bool p_readOnly)
            {
                m_list = p_list;
                m_first = Math.Max(p_first, 0);
                m_last = p_last;
                m_readOnly = p_readOnly;
            }
            public int Count
            {
                get 
                {
                    int last = Math.Min(m_last, m_list.Count - 1);
                    //return (m_first <= last) ? last - m_first + 1 : 0;    // the same without branching:
                    last -= m_first;
                    return ((last >> 31) | last) + 1;
                }
            }
            public T this[int index]
            {
                get { return m_list[m_first + index]; }
                set
                {
                    if (IsReadOnly)
                        throw new NotSupportedException();
                    m_list[m_first + index] = value; 
                }
            }
            public bool IsReadOnly          { get { return m_readOnly; } }
            public bool Contains(T item)    { return IndexOf(item) >= 0; }
            public void Add(T item)         { Insert(Count, item); }
            public void Clear()             { m_last = m_first - 1; }
            public void RemoveAt(int index)
            {
                if (IsReadOnly)
                    throw new NotSupportedException();
                m_list.RemoveAt(m_first + index);
                m_last -= 1;
            }

            public int IndexOf(T item)
            {
                int last = Math.Min(m_last, m_list.Count - 1);
                for (int i = m_first; i <= m_last; ++i)
                    if (EqualityComparer<T>.Default.Equals(m_list[m_first + i], item))
                        return i - m_first;
                return -1;
            }

            public void CopyTo(T[] array, int arrayIndex)
            {
                int last = Math.Min(m_last, m_list.Count - 1);
                for (int i = m_first; i <= last; ++i)
                    array[arrayIndex++] = m_list[i];
            }

            public void Insert(int index, T item)
            {
                if (IsReadOnly)
                    throw new NotSupportedException();
                if (unchecked((uint)index > (uint)Count))
                    throw new ArgumentOutOfRangeException();
                m_list.Insert(m_first + index, item);
                m_last += 1;
            }

            public bool Remove(T item)
            {
                if (IsReadOnly)
                    throw new NotSupportedException();
                int idx = IndexOf(item);
                if (idx < 0)
                    return false;
                RemoveAt(idx);
                return true;
            }

            public IEnumerator<T> GetEnumerator()
            {
                for (int i = 0; i < Count; ++i)
                    yield return m_list[m_first + i];
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
        #endregion
        #region TimeInterval
        /// <summary> Returns a list containing dates from p_first until p_last (inclusive) 
        /// with p_step steps. </summary>
        public static AbstractList<DateTime> TimeInterval(DateTime p_first, DateTime p_last, TimeSpan p_step)
        {
            return new TimeIntervalClass(p_first, p_last, p_step);
        }

        class TimeIntervalClass : AbstractList<DateTime>
        {
            DateTime m_a;
            long m_step;
            int m_count;
            internal TimeIntervalClass(DateTime p_a, DateTime p_b, TimeSpan p_step)
            {
                m_a = p_a;
                m_step = p_step.Ticks;
                m_count = checked((int)((p_b - p_a).Ticks / m_step + 1));
            }
            public override int Count
            {
                get { return m_count; }
            }
            public override DateTime this[int index]
            {
                get
                {
                    return m_a.AddTicks(index * m_step);
                }
                set { throw new NotSupportedException(); }
            }
        }
        #endregion
        #region Factory
        /// <summary> Returns a list containing p_count new instances of T.
        /// New instances are created on read operations. </summary>
        public static AbstractList<T> Factory<T>(int p_count) where T : new()
        {
            return new FactoryClass<T>(p_count);
        }

        public class FactoryClass<T> : AbstractList<T> where T : new()
        {
            int m_count;
            public FactoryClass(int p_count) { m_count = Math.Max(0, p_count); }
            public override int Count
            {
                get { return m_count; }
            }
            public override T this[int index]
            {
                get
                {
                    return new T();
                }
                set { throw new NotSupportedException(); }
            }
        }
        #endregion
        #region ComparerFromComparison
        public static ComparerFromComparisonStruct<T> GetComparerFromComparison<T>(Comparison<T> p_cmp)
        {
            return new ComparerFromComparisonStruct<T>(p_cmp);
        }
        public struct ComparerFromComparisonStruct<T> : IComparer<T>, System.Collections.IComparer
        {
            public readonly Comparison<T> m_comparer;
            public ComparerFromComparisonStruct(Comparison<T> p_cmp) { m_comparer = p_cmp; }
            // the following ctor can be used to convert from IComparer<> to IComparer
            public ComparerFromComparisonStruct(IComparer<T> p_cmp) { m_comparer = p_cmp.Compare; }
            public int Compare(T x, T y)
            {
                return m_comparer(x, y);
            }
            int System.Collections.IComparer.Compare(object x, object y)
            {
                return m_comparer((T)x, (T)y);
            }
        }
        public static IComparer<T> GetDescendingComparer<T>(IComparer<T> p_cmp)
        {
            if (p_cmp == null)
                p_cmp = Comparer<T>.Default;
            return GetComparerFromComparison<T>((x, y) => p_cmp.Compare(y, x));
        }

        #endregion
        #region BinarySearch
        public static int BinarySearch<T>(IList<T> p_list, T p_key)
        {
            return BinarySearch<T,T>(p_list, 0, p_list.Count, p_key, Comparer<T>.Default.Compare, true);
        }
        public static int BinarySearch<T>(IList<T> p_list, T p_key, IComparer<T> p_comp)
        {
            return BinarySearch<T,T>(p_list, 0, p_list.Count, p_key, p_comp.Compare, true);
        }
        public static int BinarySearch<T>(IList<T> p_list, T p_key, Comparison<T> p_comp)
        {
            var cmp = (Comparison<T,T>)Delegate.CreateDelegate(typeof(Comparison<T,T>), p_comp.Target, p_comp.Method);
            return BinarySearch<T,T>(p_list, 0, p_list.Count, p_key, cmp, true);
        }
        public static int BinarySearch<T, K>(IList<T> p_list, K p_key, System.Collections.IComparer p_comp)
        {
            return BinarySearch(p_list, 0, p_list.Count, p_key,
                (listItem, key) => p_comp.Compare(listItem, key), true);
        }
        public static int BinarySearch<T, K>(IList<T> p_list, K p_key, Comparison<T, K> p_comp)
        {
            return BinarySearch(p_list, 0, p_list.Count, p_key, p_comp, true);
        }

        /// <summary> Searches for p_key in the range of elements of p_list[]
        /// specified by p_first and p_count (negative p_count specifies all
        /// elements until the end of p_list[]).
        /// Returns the index where p_key is found (the highest index if
        /// p_unique is false), or a negative number which is the bitwise
        /// complement of the smallest index at which the element is larger,
        /// or ~p_count if there's no larger element ( ~p_list.Count is used
        /// when p_count is negative).
        /// Note: ~x == -x - 1,   ~x - 1 == ~++x. </summary>
        public static int BinarySearch<T, K>(IList<T> p_list, int p_first,
            int p_count, K p_key, Comparison<T, K> p_comparisonTK, bool p_unique)
        {
            if (p_comparisonTK == null)
                throw new ArgumentNullException();
            int n = (p_count < 0) ? p_list.Count - p_first : p_count;
            if (n <= 0)
                return -1;
            int a = p_first, b = --n + p_first, tst = -1;
            while (a < b)
            {
                int k = (a + b + 1) >> 1;
                tst = p_comparisonTK(p_list[k], p_key);
                if (tst > 0)
                    b = k - 1;
                else
                {
                    a = k;
                    if (tst == 0 && p_unique)
                        break;
                }
            }
            if (tst != 0)
                tst = p_comparisonTK(p_list[a], p_key);
            if (tst != 0)
                a = (tst > 0) ? ~a : ~(a + 1);
            return a;
        }

        #endregion

        /// <summary> Helper class for internal customizable dictionaries.
        /// Intended usage:
        ///    static readonly object KeyForAField = new Utils.HashKey("debug name");
        /// </summary>
        public class HashKey
        {
            static int g_lastHashCode = -1;
            private readonly int m_hashCode;
            /// <summary> "new HashKey()" is almost equivalent to "new object()", but
            /// in DEBUG builds, ToString() prints a #99 number instead of 'Object'.
            /// This makes it easier to differentiate instances during debugging. </summary>
            public HashKey()
            {
                m_hashCode = System.Threading.Interlocked.Increment(ref g_lastHashCode);
            }
            /// <summary> "new HashKey(string)" is similar to "new object()", except
            /// that in DEBUG builds the string is returned by ToString().
            /// In RELEASE builds, the string is ignored. </summary>
#if DEBUG
            public HashKey(string p_debugString) : this()
            {
                m_debugString = p_debugString; 
            }
            private readonly string m_debugString;
            public override string ToString()   // for debugging only
            {
                return m_debugString ?? '#' + m_hashCode.ToString();
            }
#else
            public HashKey(string p_debugString) : this() { }
#endif
            public override int GetHashCode()       { return m_hashCode; }
            public override bool Equals(object obj) { return base.Equals(obj); }
        }
    }

    /// <summary> Combines hash codes of several (heterogeneous) values
    /// -- without boxing.
    /// Example: return new CompositeHashCode { val1, val2, val3 };
    /// (val1, val2, val3 may be of different types).
    /// Note: IEnumerable implementation is fake, it is only implemented
    /// because the compiler requires it for the collection initializer
    /// syntax </summary>
    public struct CompositeHashCode : System.Collections.IEnumerable
    {
        public int Result;
        public CompositeHashCode(System.Collections.IEnumerable p_seq)
        {
            Result = 0;
            AddAll(p_seq);
        }
        public CompositeHashCode Add<T>(T p_item)
        {
            // 92821 taken from stackoverflow.com/a/2816747
            Result += unchecked((Result * 92821) ^ (p_item == null ? 0 : p_item.GetHashCode()));
            return this;
        }
        public CompositeHashCode AddAll(System.Collections.IEnumerable p_seq)
        {
            if (p_seq != null)
                foreach (object o in p_seq)
                    Add(o);
            return this;
        }
        public static implicit operator int(CompositeHashCode p_this) { return p_this.Result; }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return null; }
    }

    /// <summary> Example: return new MaxOfMany&lt;int&gt; { a, b, 23 };
    /// Note: IEnumerable implementation is fake, it is done because the
    /// compiler requires it for the collection initializer syntax </summary>
    public struct MaxOfMany<T> : System.Collections.IEnumerable
    {
        public T MaxValue;
        public int Count, Index;
        public void Add(T p_item)
        {
            if (Count == 0 || Comparer<T>.Default.Compare(MaxValue, p_item) < 0)
            {
                MaxValue = p_item;
                Index = Count;
            }
            ++Count;
        }
        public static implicit operator T(MaxOfMany<T> p_this) { return p_this.MaxValue; }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return null; }
    }

#region AbstractList<T>, AbstractCollection<T>
    /// <summary> Implements most of the IList&lt;&gt; interface, 
    /// to facilitate special IList implementations. </summary>
    [DebuggerDisplay("Count = {Count}")]
    [DebuggerTypeProxy("System.Collections.Generic.Mscorlib_CollectionDebugView`1, mscorlib")]
    public abstract class AbstractList<T> : IList<T>, System.Collections.ICollection, IReadOnlyList<T>
    {
        public abstract int Count { get; }
        public abstract T this[int index] { get; set; }
        protected object m_syncRoot;

        public virtual bool IsReadOnly
        {
            get { return true; }
            set { throw new NotSupportedException(); }
        }

        /// <summary> Calls p_item.Equals(item_in_list) </summary>
        public virtual int IndexOf(T p_item)
        {
            IEqualityComparer<T> cmp = EqualityComparer<T>.Default;
            for (int i = 0, n = Count; i < n; ++i)
                if (cmp.Equals(p_item, this[i]))
                    return i;
            return -1;
        }

        public virtual void Insert(int index, T item)
        {
            throw IsReadOnly ? (SystemException)new NotSupportedException() 
                             : (SystemException)new NotImplementedException();
        }

        public virtual void RemoveAt(int index)
        {
            throw IsReadOnly ? (SystemException)new NotSupportedException() 
                             : (SystemException)new NotImplementedException();
        }

        public virtual void Add(T item)
        {
            Insert(Count, item);
        }

        public virtual void Clear()
        {
            for (int i = Count - 1; i >= 0; --i)
                RemoveAt(i);
        }

        public virtual bool Contains(T item)
        {
            return IndexOf(item) >= 0;
        }

        public virtual void CopyTo(T[] array, int arrayIndex)
        {
            for (int i = 0, n = Count; i < n; ++i)
                array[arrayIndex++] = this[i];
        }

        public void CopyTo(Array array, int arrayIndex)
        {
            T[] ta = array as T[];
            if (ta != null)
                CopyTo(ta, arrayIndex);
            else for (int i = 0, n = Count; i < n; ++i)
                array.SetValue(this[i], arrayIndex++);
        }

        public virtual T[] ToArray()
        {
            T[] result = new T[Count];
            CopyTo(result, 0);
            return result;
        }

        public virtual bool Remove(T item)
        {
            int idx = IndexOf(item);
            if (idx >= 0)
            {
                RemoveAt(idx);
                return true;
            }
            return false;
        }

        public virtual IEnumerator<T> GetEnumerator()
        {
            return Iterate().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        protected virtual IEnumerable<T> Iterate()
        {
            for (int i = 0; i < Count; ++i)
                yield return this[i];
        }

        public virtual bool IsSynchronized
        {
            get { return false; }
        }

        public virtual object SyncRoot
        {
            get
            {
                if (m_syncRoot == null)
                    System.Threading.Interlocked.CompareExchange(ref m_syncRoot, new object(), null);
                return m_syncRoot;
            }
        }
    }

    /// <summary> Facilitates implementing read-only collections. </summary>
    [DebuggerTypeProxy("System.Collections.Generic.Mscorlib_CollectionDebugView`1, mscorlib")]
    public abstract class AbstractCollection<T> : ICollection<T>, System.Collections.ICollection
    {
        protected object m_syncRoot;

        public abstract int Count { get; }
        public abstract IEnumerator<T> GetEnumerator();

        /// <summary> Calls p_item.Equals(item_in_list) </summary>
        public virtual bool Contains(T p_item)
        {
            foreach (T t in this)
                if (EqualityComparer<T>.Default.Equals(p_item, t))
                    return true;
            return false;
        }

        public virtual void CopyTo(T[] array, int arrayIndex)
        {
            foreach (T t in this)
                array[arrayIndex++] = t;
        }

        public virtual void CopyTo(Array array, int index)
        {
            foreach (T t in this)
                array.SetValue(t, index++);
        }

        public virtual bool IsReadOnly      { get { return true;  } }
        public virtual bool IsSynchronized  { get { return false; } }
        public virtual void Add(T item)     { NoSuchOp(); }
        public virtual void Clear()         { NoSuchOp(); }
        public virtual bool Remove(T item)  { return NoSuchOp(); }
        bool NoSuchOp()
        {
            throw IsReadOnly ? (SystemException)new NotSupportedException() 
                             : (SystemException)new NotImplementedException();
        }
        public virtual object SyncRoot
        {
            get
            {
                if (m_syncRoot == null)
                    System.Threading.Interlocked.CompareExchange(ref m_syncRoot, new object(), null);
                return m_syncRoot;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
#endregion

    /// <summary> Writing through to the items of m_baseList[] is disabled by default,
    /// because it would allow reordering/modifying the items.
    /// </summary>
    public class ListView<T> : AbstractList<T>
    {
        readonly IList<T> m_baseList;
        readonly IList<int> m_indices;
        bool m_isWritingAllowed;    // default is false

        public ListView(IList<T> p_baseList, IList<int> p_indices, bool p_allowWrites = false)
        {
            m_baseList = p_baseList;
            m_indices = p_indices;
            m_isWritingAllowed = p_allowWrites;
        }

        public override int Count { get { return m_indices.Count; } }
        public override T this[int index]
        {
            get { return m_baseList[m_indices[index]]; }
            set 
            {
                if (m_isWritingAllowed)
                    m_baseList[m_indices[index]] = value;
                else
                    throw new InvalidOperationException();
            }
        }
        protected override IEnumerable<T> Iterate() // optimization for the case when m_indices[] is a FastGrowingList
        {
            foreach (int i in m_indices)
                yield return m_baseList[i];
        }
    }

    public interface IListItemSelector<in T1, out T2>
    {
        // Note: the list is not passed, intentionally! Exploited in (for example) AccessOrderCache...GetValuesHelper class
        T2 GetAt(int p_index, T1 p_item);
    }

    public struct DefaultListItemSelector<T1, T2> : IListItemSelector<T1, T2>
    {
        public Func<int, T1, T2> Delegate;
        public DefaultListItemSelector(Func<int, T1, T2> p_delegate) { Delegate = p_delegate; }
        public T2 GetAt(int p_index, T1 p_item) { return Delegate(p_index, p_item); }
        public static implicit operator DefaultListItemSelector<T1, T2>(Func<int, T1, T2> p_delegate)
        {
            return new DefaultListItemSelector<T1, T2>(p_delegate);
        }
    }

    public static class Empty<TArray> where TArray : System.Collections.IList, ICloneable, System.Collections.IStructuralEquatable
    {
        public static readonly TArray _ = (TArray)(object)Array.CreateInstance(typeof(TArray).GetElementType(), 0);
    }

    public interface IArrayBasedCollection<out T>
    {
        int Count { get; }
        T[] Array { get; }
    }

    [DebuggerDisplay("Count = {Count}")]
    public struct QuicklyClearableList<T> : IList<T>, IArrayBasedCollection<T>
    {
        public T[] m_array;
        public int m_count;

        /// <summary> It uses Utils.AsArray() (does not copy items if possible) </summary>
        public static QuicklyClearableList<T> TakeArray(IEnumerable<T> p_seq)
        {
            var result = default(QuicklyClearableList<T>);
            if (0 < Utils.TryGetCount(p_seq))
                result.m_count = (result.m_array = Utils.AsArray(p_seq)).Length;
            else if (p_seq != null)
                foreach (T item in p_seq)
                    result.Add(item);
            return result;
        }

        public void Add(T p_item)
        {
            if (m_array == null)
                m_array = new T[16];
            else if (m_array.Length <= m_count)
                Array.Resize(ref m_array, Math.Max(4, m_array.Length * 2));
            m_array[m_count++] = p_item;
        }
        public void AddRange(IEnumerable<T> p_seq)
        {
            var c = p_seq as ICollection<T>; int n;
            if (c == null)
                foreach (T item in p_seq.EmptyIfNull())
                    Add(item);
            else if (0 < (n = c.Count))
            {
                Capacity = Math.Max(Capacity, Count + n);
                c.CopyTo(m_array, m_count); m_count += n;
            }
        }
        public void Clear() { m_count = 0; }
        /// <summary> Does nothing if p_capacity &lt;= 0 </summary>
        public QuicklyClearableList<T> EnsureCapacity(int p_capacity)
        {
            if (Capacity < p_capacity)
                Capacity = p_capacity;
            return this;
        }
        public QuicklyClearableList<T> EnsureCapacity(int p_capacity, int p_adjustmentPowerOf2)
        {
            if (Capacity < p_capacity)
            {
                if (0 <= --p_adjustmentPowerOf2)
                    p_capacity = checked((int)((Math.Max((long)Capacity << 1, p_capacity) + p_adjustmentPowerOf2) & ~p_adjustmentPowerOf2));
                Capacity = p_capacity;
            }
            return this;
        }
        public int Capacity
        {
            get { return m_array == null ? 0 : m_array.Length; }
            set
            {
                if (value == 0)
                {
                    m_count = 0;
                    m_array = null;
                }
                else if (m_array == null || m_array.Length < value)
                {
                    var tmp = new T[value];
                    if (m_array != null && 0 < m_count)
                        Array.Copy(m_array, tmp, m_count);
                    m_array = tmp;
                }
                else
                {
                    if (value < m_count)
                        m_count = value;
                    Array.Resize(ref m_array, value);
                }
            }
        }
        public int Count 
        { 
            get { return m_count; }
            set
            {
                if (m_count < value && Capacity < value)
                    Capacity = value;
                m_count = value;
            }
        }
        public KeyValuePair<bool, T> Last
        {
            get 
            {
                return (m_count <= 0) ? default(KeyValuePair<bool, T>)
                    : new KeyValuePair<bool, T>(true, m_array[m_count - 1]);
            }
        }
        /// <summary> Returns null if empty </summary>
        public T[] TrimExcess()
        {
            if (m_count == 0)
                m_array = null;
            else if (m_count < m_array.Length)
                Array.Resize(ref m_array, m_count);
            return m_array;
        }


        #region IList<T> Members
        public bool IsReadOnly       { get { return false; } }
        public int IndexOf(T item)   { return (m_count <= 0) ? -1 : Array.IndexOf(m_array, item, 0, m_count); }
        public bool Contains(T item) { return IndexOf(item) >= 0; }
        public T this[int index]
        {
            get { return m_array[index]; }
            set { m_array[index] = value; }
        }
        public void Insert(int index, T item)
        {
            if (m_count <= index)
                Add(item);
            else
            {
                Add(m_array[m_count - 1]);
                index &= ~(index >> 31);        // index = Max(0, index)
                if (0 < m_count - index - 2)
                    Array.Copy(m_array, index, m_array, index + 1, m_count - index - 2);
                m_array[index] = item;
            }
        }
        public void FastRemoveAt(int p_index)
        {
            if (unchecked((uint)p_index < (uint)m_count) && p_index < --m_count)
                m_array[p_index] = m_array[m_count];
        }
        public void RemoveAt(int index)
        {
            if (unchecked((uint)index >= (uint)m_count))
                return;
            if (index < m_count - 1)
                Array.Copy(m_array, index + 1, m_array, index, m_count - index - 1);
            --m_count;
        }
        public void CopyTo(T[] array, int arrayIndex)
        {
            if (0 < m_count)
                Array.Copy(m_array, 0, array, arrayIndex, m_count);
        }
        public bool Remove(T item)
        {
            int idx = IndexOf(item);
            if (idx < 0)
                return false;
            RemoveAt(idx);
            return true;
        }
        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < m_count; ++i)
                yield return m_array[i];
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        #endregion

        /// <summary> Returns the number of items removed </summary>
        public int RemoveRange(int p_start, int p_count)
        {
            if (p_count <= 0 || m_count <= (p_start & ~(p_start >> 31)))    // Max(0, p_start)
                return 0;
            int n = m_count, nn = n - (p_start + p_count); nn &= ~(nn >> 31);
            if (0 < nn)
                Array.Copy(m_array, p_start + p_count, m_array, p_start & ~(p_start >> 31), nn);
            m_count = (p_start & ~(p_start >> 31)) + nn;
            return n - m_count;
        }

        T[] IArrayBasedCollection<T>.Array { get { return m_array; } }
    }

    //public class Pairs<K, V> : List<KeyValuePair<K, V>>
    //{
    //    public Pairs() { }
    //    public Pairs(int p_capacity) : base(p_capacity) { }
    //    public Pairs(IEnumerable<KeyValuePair<K, V>> p_seq) : base(p_seq) { }
    //    public void Add(K p_key, V p_value) { base.Add(new KeyValuePair<K,V>(p_key, p_value)); }
    //    public ICollection<K> Keys
    //    {
    //        get { return Utils.MakeCollection(GetKeys(), Count); }
    //    }
    //    public ICollection<V> Values
    //    {
    //        get { return Utils.MakeCollection(GetValues(), Count); }
    //    }
    //    private IEnumerable<K> GetKeys()
    //    {
    //        for (int i = 0; i < Count; ++i)
    //            yield return this[i].Key;
    //    }
    //    private IEnumerable<V> GetValues()
    //    {
    //        for (int i = 0; i < Count; ++i)
    //            yield return this[i].Value;
    //    }
    //}

#region ISet<>, Set<>
    /*
    public interface ISet<T> : ICollection<T>
    {
        bool IsEmpty { get; }
        void AddAll(IEnumerable<T> p_src);
        void RemoveAll(IEnumerable<T> p_src);
    }

    /// <summary> General set, based on a hash table or tree dictionary. </summary>
    public class Set<T> : ISet<T>
    {
        protected System.Collections.IDictionary m_dictionary;

        public Set() : this(new System.Collections.Hashtable())
        {
        }

        public Set(System.Collections.IDictionary p_dictionary)
        {
            if (p_dictionary == null)
                p_dictionary = new System.Collections.Hashtable();
            m_dictionary = p_dictionary;
        }

        public Set(int p_capacity)
            : this(new System.Collections.Hashtable(p_capacity))
        {
        }

        public Set(System.Collections.IEqualityComparer p_eqComparer)
            : this(new System.Collections.Hashtable(p_eqComparer))
        {
        }

        public Set(Comparer<T> p_comparer)
            : this((System.Collections.IDictionary)(new SortedDictionary<T, object>(p_comparer)))
        {
        }

        public Set(IEnumerable<T> p_source)
            : this(p_source, false)
        {
        }

        public Set(IEnumerable<T> p_source, bool p_isSorted)
            : this(p_isSorted ? (System.Collections.IDictionary)new SortedDictionary<T, object>()
                              : (System.Collections.IDictionary)new System.Collections.Hashtable())
        {
            AddAll(p_source);
        }

        public bool IsEmpty
        {
            get { return Count == 0; }
        }

        public virtual void AddAll(IEnumerable<T> p_src)
        {
            foreach (T item in p_src)
                Add(item);
        }

        public virtual void RemoveAll(IEnumerable<T> p_src)
        {
            foreach (T item in p_src)
                Remove(item);
        }

        public virtual void Add(T item)
        {
            m_dictionary[item] = item;
        }

        public virtual void Clear()
        {
            m_dictionary.Clear();
        }

        public virtual bool Contains(T item)
        {
            return m_dictionary.Contains(item);
        }

        /// <summary> Uses the Set's equality comparer, not the Equals() 
        /// and GetHashCode() methods of p_key. If p_key is found, true
        /// is returned and p_item is set to the found element. </summary>
        public virtual bool TryGetItem(object p_key, out T p_item)
        {
            if (!m_dictionary.Contains(p_key))
            {
                p_item = default(T);
                return false;
            }
            p_item = (T)m_dictionary[p_key];
            return true;
        }

        public virtual T this[object p_key]
        {
            get { T result; TryGetItem(p_key, out result); return result; }
        }

        public virtual void CopyTo(T[] array, int arrayIndex)
        {
            m_dictionary.CopyTo(array, arrayIndex);
        }

        public virtual int Count
        {
            get { return m_dictionary.Count; }
        }

        public bool IsReadOnly
        {
            get { return m_dictionary.IsReadOnly; }
        }

        public virtual bool Remove(T item)
        {
            bool ans = m_dictionary.Contains(item);
            if (ans) m_dictionary.Remove(item);
            return ans;
        }

        public virtual IEnumerator<T> GetEnumerator()
        {
            return new Iterator(this);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return new Iterator(this);
        }

        class Iterator : IEnumerator<T>, System.Collections.IEnumerator
        {
            Set<T> m_owner;
            System.Collections.IEnumerator m_it = null;

            internal Iterator(Set<T> p_owner)
            {
                m_owner = p_owner;
            }

            public object Current
            {
                get { return m_it.Current; }
            }

            T IEnumerator<T>.Current
            {
                get { return (T)m_it.Current; }
            }

            public bool MoveNext()
            {
                if (m_owner != null)
                {
                    if (m_it == null)
                        m_it = m_owner.m_dictionary.Keys.GetEnumerator();
                    return m_it.MoveNext();
                }
                return false;
            }

            public void Reset()
            {
                m_it = null;
            }

            public void Dispose()
            {
                m_owner = null;
                m_it = null;
            }
        }
    }
    */
#endregion

    public enum DestructorBehavior : sbyte
    {
        Unset,
        DoNothing,

        /// <summary> This should only be used when the resource to be disposed
        /// by the dtor does not have own dtor or Finalize() method (e.g. unmanaged) 
        /// </summary>
        DoNothingWhenProcessShutdown,

        /// <summary> Use this only when the Dispose() routine of the resource 
        /// to be disposed by the dtor is aware of Environment.HasShutdownStarted
        /// and won't access any global object. Furthermore, this setting should 
        /// only be used when the resource does not have own dtor or Finalize() 
        /// method (e.g. unmanaged). </summary>
        ExecuteEvenIfProcessShutdown
    }

    public sealed class Iterator<T> : IEnumerator<T>, System.Collections.IEnumerator, IEnumerable<T>
    {
        IEnumerator<T> m_enumerator;
        bool m_started, m_ended;
        DestructorBehavior m_dtorBehavior = DestructorBehavior.DoNothingWhenProcessShutdown;

        public Iterator(IEnumerator<T> p_enumerator, bool p_isStarted, bool p_isEnded)
        {
            m_started = p_isStarted;
            m_ended = p_isEnded || (p_enumerator == null);
            m_enumerator = p_enumerator;
        }

        public Iterator(IEnumerable<T> p_enumerable)
            : this(p_enumerable == null ? null : p_enumerable.GetEnumerator(), false, false)
        {
        }

        ~Iterator()
        {
            if (m_dtorBehavior == DestructorBehavior.ExecuteEvenIfProcessShutdown
                || !Environment.HasShutdownStarted)
                Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /*protected*/ void Dispose(bool p_notFromFinalize)
        {
            if (m_enumerator != null)
            {
                IEnumerator<T> tmp = m_enumerator;
                m_enumerator = null;
                tmp.Dispose();
            }
        }

        /// <summary> The default is DoNothingWhenProcessShutdown </summary>
        public DestructorBehavior DtorBehavior 
        {
            get { return m_dtorBehavior; }
            set
            {
                if (value == DestructorBehavior.Unset || m_dtorBehavior == value)
                    return;
                if (m_dtorBehavior == DestructorBehavior.DoNothing && m_enumerator != null)
                    GC.ReRegisterForFinalize(this);
                m_dtorBehavior = value;
                if (value == DestructorBehavior.DoNothing)
                    GC.SuppressFinalize(this);
            }
        }

        public bool MoveNext()
        {
            m_started = true;
            if (m_enumerator != null && m_enumerator.MoveNext()) 
            {
                m_ended = false;
                return true;
            }
            m_ended = true;
            return false;
        }

        public void Reset()
        {
            if (m_enumerator != null)
            {
                m_enumerator.Reset();
                m_started = m_ended = false;
            }
            else
            {
                m_started = false;
                m_ended = true;
            }
        }

        public T Current
        {
            get { return m_enumerator.Current; }
        }

        object System.Collections.IEnumerator.Current
        {
            get { return m_enumerator.Current; }
        }

        /// <summary> Does not move the enumerator if it is already started, 
        /// or starts it if it has not been started yet. Returns !IsEnded </summary>
        /// <returns></returns>
        public bool Continue()
        {
            if (m_ended)
                return false;
            if (m_started)
                return true;
            return MoveNext();
        }

        public bool IsStarted
        {
            get { return m_started; }
        }

        public bool IsEnded
        {
            get { return m_ended; }
        }

        /// <summary> 
        /// This method takes the returned enumerator object out of <c>this</c> object,
        /// so <c>this</c> is not responsible for that object any longer and does not 
        /// control that. The caller is responsible for Disposing the returned object. 
        /// <c>this</c> object becomes invalid, as if <c>this.Dispose()</c> would have 
        /// been called.<br/>
        /// This is done because this object cannot know any longer that the returned 
        /// enumerator is still in use or isn't, and therefore need to call Dispose() 
        /// on it or needn't. </summary>
        public IEnumerator<T> GetEnumerator()
        {
            IEnumerator<T> result = m_enumerator;
            m_enumerator = null;
            GC.SuppressFinalize(this);
            return result;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            System.Collections.IEnumerator result = m_enumerator;
            m_enumerator = null;
            GC.SuppressFinalize(this);
            return result;
        }
    }

    public class Disposables : List<IDisposable>, IDisposable
    {
        public Disposables() { }
        public Disposables(int p_capacity) : base(p_capacity) { }
        public Disposables(IEnumerable<IDisposable> p_seq) : base(p_seq) { }

        public T Add<T>(T p_disposable) where T : IDisposable
        {
            base.Add(p_disposable);
            return p_disposable;
        }

        ~Disposables()
        {
            Dispose(false);
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool p_notFromFinalize)
        {
            try     { Utils.DisposeAll(this); }
            finally { Clear(); }
        }
    }

    /// <summary> Use this class to convert an IComparer to an IComparer&lt;&gt; </summary>
    public class GenericComparer<T> : IComparer<T>, System.Collections.IComparer
    {
        public readonly System.Collections.IComparer m_comparer;
        public GenericComparer(System.Collections.IComparer p_cmp) { m_comparer = p_cmp; }
        public int Compare(T x, T y)
        {
            return m_comparer.Compare(x, y);
        }
        int System.Collections.IComparer.Compare(object x, object y)
        {
            return m_comparer.Compare(x, y);
        }
    }

    public class DescendingComparer<T> : IComparer<T> where T : IComparable<T>
    {
        public int Compare(T x, T y) { return y.CompareTo(x); }
        public static readonly DescendingComparer<T> Default = new DescendingComparer<T>();
    }

    //public class ReferenceEqComparer : IEqualityComparer<object>, System.Collections.IEqualityComparer
    //{
    //    public static readonly ReferenceEqComparer Default = new ReferenceEqComparer();
    //    public int GetHashCode(object obj)      { return (obj == null) ? 0 : obj.GetHashCode(); }
    //    public bool Equals(object x, object y)  { return x == y; }
    //}
    //public struct ReferenceEqWrapper<T> : IEquatable<ReferenceEqWrapper<T>>
    //{
    //    object m_obj;
    //    public ReferenceEqWrapper(object p_value) { m_obj = p_value; }
    //    public T Value                       { get { return (T)m_obj; } set { m_obj = value; } }
    //    public override string ToString()    { return Utils.ToStringOrNull(m_obj); }
    //    public override int GetHashCode()    { return (m_obj == null) ? 0 : m_obj.GetHashCode(); }
    //    public override bool Equals(ReferenceEqWrapper<T> p_other) { return p_other.m_obj == m_obj; }
    //    public override bool Equals(object obj)
    //    {
    //        return (obj is ReferenceEqWrapper<T>) ? Equals((ReferenceEqWrapper<T>)obj)
    //                                              : obj == m_obj;
    //    }
    //}

    /// <summary> Helper struct for using non-generic IComparables (like enums)
    /// as IComparable&lt;&gt; </summary>
    public struct GenericComparable<V> : IComparable, IComparable<V>, IEquatable<V>, 
        IComparable<GenericComparable<V>>, IEquatable<GenericComparable<V>>
        where V : IComparable
    {
        public V Value;     // no ctor - use object initializer to set this member
        public int CompareTo(object p_other)
        {
            if (Value == null)
                return System.Collections.Comparer.Default.Compare(Value, p_other);
            return Value.CompareTo(p_other);
        }
        public int CompareTo(V p_other)
        {
            if (Value == null)
                return p_other == null ? 0 : -p_other.CompareTo(Value);
            return Comparer<V>.Default.Compare(Value, p_other);
        }
        public int CompareTo(GenericComparable<V> p_other)
        {
            return CompareTo(p_other.Value);
        }
        public bool Equals(GenericComparable<V> p_other)
        {
            return EqualityComparer<V>.Default.Equals(Value, p_other.Value);
        }
        public bool Equals(V p_other)
        {
            return EqualityComparer<V>.Default.Equals(Value, p_other);
        }
        public override bool Equals(object obj)
        {
            return (obj is GenericComparable<V>) ? Equals((GenericComparable<V>)obj)
                                                 : ((obj is V) && Equals((V)obj));
        }
        public override int GetHashCode()
        {
            return Value == null ? 0 : Value.GetHashCode();
        }
        public override string ToString()
        {
            return Value == null ? null : Value.ToString();
        }
        public static bool operator ==(GenericComparable<V> p_v1, GenericComparable<V> p_v2) { return p_v1.Equals(p_v2); }
        public static bool operator ==(GenericComparable<V> p_v1, V p_v2)                    { return p_v1.Equals(p_v2); }
        public static bool operator ==(V p_v1, GenericComparable<V> p_v2)                    { return p_v2.Equals(p_v1); }
        public static bool operator !=(GenericComparable<V> p_v1, GenericComparable<V> p_v2) { return !p_v1.Equals(p_v2); }
        public static bool operator !=(GenericComparable<V> p_v1, V p_v2)                    { return !p_v1.Equals(p_v2); }
        public static bool operator !=(V p_v1, GenericComparable<V> p_v2)                    { return !p_v2.Equals(p_v1); }
        public static implicit operator V(GenericComparable<V> p_this)      { return p_this.Value; }
        public static implicit operator GenericComparable<V>(V p_value)     { return new GenericComparable<V> { Value = p_value }; }
    }

/*
    /// <summary> Helper struct for using IComparable&lt;&gt; objects as IComparables </summary>
    public struct ComparableWrapper<T1, T2> : IComparable, IComparable<T1>, IComparable<ComparableWrapper<T1, T2>>
        where T1 : IComparable<T2>
    {
        public T1 Value;
        public int CompareTo(object p_other)
        {
            if (ReferenceEquals(Value, null))
                return System.Collections.Comparer.Default.Compare(null, p_other);
            return Value.CompareTo((T2)p_other);
        }
        public int CompareTo(T1 p_other)
        {
            return CompareTo((object)p_other);
        }
        public int CompareTo(ComparableWrapper<T1, T2> p_other)
        {
            return CompareTo((object)p_other.Value);
        }
        public bool Equals(ComparableWrapper<T1, T2> p_other)
        {
            return Equals(Value, p_other.Value);
        }
        public bool Equals(T1 p_other)
        {
            return Equals(Value, p_other);
        }
        public override bool Equals(object obj)
        {
            return (obj is ComparableWrapper<T1, T2>) ? Equals((ComparableWrapper<T1, T2>)obj)
                                                      : ((obj is T1) && Equals((T1)obj));
        }
        public override int GetHashCode()
        {
            return ReferenceEquals(Value, null) ? 0 : Value.GetHashCode();
        }
        public override string ToString()
        {
            return ReferenceEquals(Value, null) ? null : Value.ToString();
        }
        public static bool operator ==(ComparableWrapper<T1, T2> p_v1, ComparableWrapper<T1, T2> p_v2)  { return p_v1.Equals(p_v2); }
        public static bool operator ==(ComparableWrapper<T1, T2> p_v1, T1 p_v2)                         { return p_v1.Equals(p_v2); }
        public static bool operator ==(T1 p_v1, ComparableWrapper<T1, T2> p_v2)                         { return p_v2.Equals(p_v1); }
        public static bool operator !=(ComparableWrapper<T1, T2> p_v1, ComparableWrapper<T1, T2> p_v2)  { return !p_v1.Equals(p_v2); }
        public static bool operator !=(ComparableWrapper<T1, T2> p_v1, T1 p_v2)                         { return !p_v1.Equals(p_v2); }
        public static bool operator !=(T1 p_v1, ComparableWrapper<T1, T2> p_v2)                         { return !p_v2.Equals(p_v1); }
        public static implicit operator T1(ComparableWrapper<T1, T2> p_this)      { return p_this.Value; }
        public static implicit operator ComparableWrapper<T1, T2>(T1 p_value)     { return new ComparableWrapper<T1, T2> { Value = p_value }; }
    }
*/

}