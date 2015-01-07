using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Diagnostics;
using System.Reflection.Emit;

namespace HQCommon
{
    public static partial class Utils
    {
        public static readonly object[] g_empty = new object[0];

        /// <summary> Creates a new instance of p_type by calling its default ctor. 
        /// Throws exception if p_type does not have a PUBLIC default ctor,
        /// or p_type cannot be converted to T. </summary>
        public static T CreateObject<T>(Type p_type)
        {
            if (ReferenceEquals(p_type, null))
                throw new ArgumentNullException();
            return MakeCtorFunc<T>(p_type)();
        }

        /// <summary> Returns a delegate that creates a new instance of p_type
        /// (null means typeof(T)) by calling its default ctor. Never returns null.
        /// Throws exception if p_type does not have a PUBLIC default ctor,
        /// or p_type cannot be converted to T. </summary>
        public static Func<T> MakeCtorFunc<T>(Type p_type = null)
        {
            return (Func<T>)MakeCtorFuncStaticCache.Get(p_type);
        }
        class MakeCtorFuncStaticCache : StaticDict<Type, Delegate, MakeCtorFuncStaticCache>
        {
            public override Delegate CalculateValue(Type p_key, object p_arg)
            {
                return (new Func<Delegate>(DefaultCtorTemplate<int>).Method.GetGenericMethodDefinition()
                    .MakeGenericMethod(p_key).Invoke(null, new object[0])) as Delegate;
            }
            static Delegate DefaultCtorTemplate<T>() where T : new() { return new Func<T>(() => new T()); }
        }

        public static MemberInfo[] GetMemberInfos(string p_memberName, object p_objectOrType, MemberTypes p_mt = MemberTypes.All)
        {
            const BindingFlags STATIC   = BindingFlags.Static   | BindingFlags.Public | BindingFlags.NonPublic;
            const BindingFlags INSTANCE = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            FieldInfo f = null;
            PropertyInfo p;
            Type t = (p_objectOrType == null) ? null : (p_objectOrType as Type ?? p_objectOrType.GetType());
            BindingFlags bf = ReferenceEquals(t, p_objectOrType) ? STATIC : INSTANCE;
            int i = 0, dot = p_memberName.IndexOf('.');
            for (; 0 <= dot && p_objectOrType != null; i = dot + 1, dot = p_memberName.IndexOf('.', i))
            {
                object value = null;
                string name = p_memberName.Substring(i, dot - i);
                foreach (var m in t.GetMember(name, MemberTypes.Field | MemberTypes.Property, bf))
                {
                    if (CanBe(m, out f))
                        value = f.GetValue(p_objectOrType);
                    else if (CanBe(m, out p) && p.CanRead)
                        value = p.GetValue(p_objectOrType, null);
                    else
                        continue;
                    break;
                }
                bf = INSTANCE;
                if ((p_objectOrType = value) != null)
                    t = p_objectOrType.GetType();
            }
            return (p_objectOrType == null) ? (MemberInfo[])Enumerable.Empty<MemberInfo>()
                                            : t.GetMember((i == 0) ? p_memberName : p_memberName.Substring(i), p_mt, bf);
        }

        /// <summary> Retrieves the value of the p_memberName property/variable/constant
        /// of p_objectOrType using reflection. Returns non-null if a member with type
        /// compatible with T is found. Returns null otherwise. Example:
        ///   Utils.GetValueOfMember("Method.Name", new Action(GC.Collect), out str);  // str:="Collect"
        /// </summary>
        public static MemberInfo GetValueOfMember<T>(string p_memberName, object p_objectOrType, out T p_value)
        {
            FieldInfo f = null; PropertyInfo p; MethodInfo mth = null;
            var isD = typeof(Delegate).IsAssignableFrom(typeof(T)) ? MemberTypes.Method : 0;
            foreach (MemberInfo m in GetMemberInfos(p_memberName, p_objectOrType, MemberTypes.Field | MemberTypes.Property | isD))
            {
                if (CanBe(m, out p) && p.CanRead && typeof(T).IsAssignableFrom(p.PropertyType))
                    p_value = (T)p.GetValue(p_objectOrType, null);
                else if (CanBe(m, out f) && typeof(T).IsAssignableFrom(f.FieldType))
                    p_value = (T)f.GetValue(p_objectOrType);
                else if (isD == 0 || !CanBe(m, out mth) || null == (p_value = (T)(object)Delegate.CreateDelegate(
                    typeof(T), (p_objectOrType is Type ? null : p_objectOrType), (MethodInfo)m, false)))
                    continue;
                return p ?? (MemberInfo)f ?? mth;
            }
            p_value = default(T);
            return null;
        }

        /// <summary> Same as the above, but this one throws InvalidOperationException 
        /// when cannot succeed (there's no such member). </summary>
        public static T GetValueOfMember<T>(string p_memberName, object p_objectOrType)
        {
            T result;
            if (GetValueOfMember<T>(p_memberName, p_objectOrType, out result) != null)
                return result;
            Type t = (p_objectOrType as Type) ?? p_objectOrType.GetType();
            throw new InvalidOperationException(String.Format("no such member: {0}.{1} of type {2}",
                t.FullName, p_memberName, typeof(T).FullName));
        }

        /// <summary> Returns a FuncWithRefArg&lt;T1, T2&gt; delegate (== func(ref T1) : T2 )
        /// where T1==p_type and T2==p_fieldOrProperty.[Field|Property]Type.
        /// The field/property may be static. </summary>
        public static Delegate EmitGetter(Type p_type, MemberInfo p_fieldOrProperty)
        {
            var f = p_fieldOrProperty as FieldInfo;
            var p = p_fieldOrProperty as PropertyInfo;
            if (f == null && p == null)
                throw new ArgumentException();
            Type t = (p != null) ? p.PropertyType : f.FieldType;
            Type td = typeof(FuncWithRefArg<,>).MakeGenericType(p_type, t);
            MethodInfo signature = td.GetMethod("Invoke");
            ParameterInfo[] pars = signature.GetParameters();
            string methodName = String.Format("Get{0}From{1}", p_fieldOrProperty.Name, p_type.Name);
            var method = new DynamicMethod(methodName, signature.ReturnType, new Type[] { pars[0].ParameterType }, p_type);
            method.DefineParameter(1, ParameterAttributes.None, pars[0].Name);  // p_op:   Ldarg_0 (because static method)
            ILGenerator il = method.GetILGenerator(8);
            if (p != null)
            {
                signature = p.GetGetMethod(true);
                if (!signature.IsStatic)
                    il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Call, signature);
            }
            else if (f.IsStatic)
                il.Emit(OpCodes.Ldsfld, f);
            else
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, f);
            }
            il.Emit(OpCodes.Ret);
            return method.CreateDelegate(td);
        }
        public static FuncWithRefArg<T1, T2> EmitGetter<T1, T2>(Expression<Func<T1, T2>> p_fieldOrPropertyExpr)
        {
            return (FuncWithRefArg<T1, T2>)EmitGetter(typeof(T1), ((MemberExpression)p_fieldOrPropertyExpr.Body).Member);
        }

        public static Type GetTypeOfMember(MemberInfo p_member)
        {
            if (p_member != null)
            {
                MemberTypes mt = p_member.MemberType;
                if ((mt & MemberTypes.Property) != 0)
                    return ((PropertyInfo)p_member).PropertyType;
                if ((mt & MemberTypes.Field) != 0)
                    return ((FieldInfo)p_member).FieldType;
                if ((mt & MemberTypes.Event) != 0)
                    return ((EventInfo)p_member).EventHandlerType;
                if ((mt & MemberTypes.Method) != 0)
                    return ((MethodInfo)p_member).ReturnType;
            }
            return null;
        }

        public static object GetValueOfMember(MemberInfo p_member, object p_instance)
        {
            MemberTypes mt = p_member.MemberType;
            if ((mt & MemberTypes.Property) != 0)
                return ((PropertyInfo)p_member).GetValue(p_instance, null);
            if ((mt & MemberTypes.Field) != 0)
                return ((FieldInfo)p_member).GetValue(p_instance);
            if ((mt & MemberTypes.Event) != 0)
                return ((EventInfo)p_member).GetRaiseMethod();
            if ((mt & MemberTypes.Method) != 0)
                return p_member;
            return null;
        }

        public static bool IsStaticMember(MemberInfo p_member)
        {
            if (p_member != null)
            {
                MemberTypes mt = p_member.MemberType;
                if ((mt & MemberTypes.Property) != 0)
                    return ((PropertyInfo)p_member).GetAccessors(true).Any(m => m.IsStatic);
                if ((mt & MemberTypes.Field) != 0)
                    return ((FieldInfo)p_member).IsStatic;
                if ((mt & MemberTypes.Event) != 0)
                    return ((EventInfo)p_member).AddMethod.IsStatic;
                if ((mt & MemberTypes.Method) != 0)
                    return ((MethodInfo)p_member).IsStatic;
            }
            throw new ArgumentException("p_member is not a property/field/event/method, it is " + 
                (p_member == null ? "null" : p_member.MemberType.ToString()));
        }

        public static void SetValueOfMember(MemberInfo p_member, object p_instance, object p_value)
        {
            int state = 0;
            if (p_member != null)
            {
                MemberTypes mt = p_member.MemberType; ++state;
                if ((mt & MemberTypes.Property) != 0)
                    ((PropertyInfo)p_member).SetValue(p_instance, p_value, null);
                else if ((mt & MemberTypes.Field) != 0)
                    ((FieldInfo)p_member).SetValue(p_instance, p_value);
                else
                    ++state;
            }
            if (p_value != null && state != 1)
                throw new ArgumentException("p_member is not a property/field, it is " + 
                    (p_member == null ? "null" : p_member.MemberType.ToString()));
        }

        #region GetNameOf(), GetMemberInfo()
        /// <summary> Utils.GetNameOf(_ => g_Hello, "g_") == "Hello"; </summary>
        public static string GetNameOf<T>(Expression<Func<int,T>> p_fieldOrPropertyExpression,
            string p_prefixToRemove = null, string p_suffixToRemove = null)
        {
            return TrimPrefix(p_prefixToRemove, TrimSuffix(GetMemberInfo<int,T>(p_fieldOrPropertyExpression).Name, p_suffixToRemove));
        }
        /// <summary> Utils.GetMemberInfo(_ => g_Hello).Name == "g_Hello"; </summary>
        public static MemberInfo GetMemberInfo<A,B>(Expression<Func<A,B>> p_fieldOrPropertyExpression)
        {
            return ((MemberExpression)p_fieldOrPropertyExpression.Body).Member;
        }
        public static MemberInfo GetMemberInfo<B>(Expression<Func<int,B>> p_fieldOrPropertyExpression)
        {
            return ((MemberExpression)p_fieldOrPropertyExpression.Body).Member;
        }
        /// <summary> Examples:<para>
        /// Utils.GetNameOfMethod((Action)Console.WriteLine) == "WriteLine";</para><para>
        /// Utils.GetNameOfMethod((Funcᐸstring,intᐳ)int.Parse) == "Parse";</para>
        /// If you need qualified name, use: <para>
        /// Utils.GetQualifiedMethodName((Funcᐸstring,intᐳ)int.Parse) == "System.Int32.Parse";</para>
        /// </summary>
        // Note: a better solution is not possible because of 1) ambiguity of overloaded methods
        // (these cannot be identified without actual arguments), 2) the stupidity of the compiler:
        // it produces error CS0411 (The type arguments for method '...' cannot be inferred from
        // the usage...) even for non-overloaded methods, e.g. A5(Buffer.BlockCopy) with A5:=
        // "static Delegate A5<A,B,C,D,E>(Action<A,B,C,D,E> d) {...}" produces CS0411..
        public static string GetNameOfMethod(Delegate p_method)
        {
            return p_method == null ? null : p_method.Method.Name;
        }
        #endregion

        #region GetCurrentMethodName(), GetQualifiedMethodName()
        /// <summary> Returns the qualified name of the current method.
        /// If p_this is given, p_this as string ?? p_this.GetType() is used instead of MethodBase.DeclaringType. </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        public static string GetCurrentMethodName(object p_this = null, bool p_omitNamespace = true)
        {
            return GetQualifiedMethodName(new StackFrame(1).GetMethod(), p_this, p_omitNamespace);
        }
        /// <summary> If p_object != null, uses p_object.ToString() or p_object.GetType() 
        /// instead of p_method.DeclaringType (.ToString() when p_object is a String/Type, .GetType() otherwise) </summary>
        // Tip: use it with System.Reflection.MethodBase.GetCurrentMethod()
        public static string GetQualifiedMethodName(MethodBase p_method, object p_object = null, bool p_omitNamespace = true)
        {
            if (p_method == null)
                return null;
            var tts = !p_omitNamespace ? t => t.ToString() : new Func<Type, string>(t => {
                if (t.IsSealed && t.IsNestedPrivate && t.IsDefined(typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute))) // <>c__DisplayClass99 and the like
                    t = t.DeclaringType;
                if (t.IsGenericType)    // return something like List`1[Int32]  instead of "List`1" (==.Name)
                    return System.Text.RegularExpressions.Regex.Replace(t.ToString(), @"[\p{L}\p{N}_.]*\.([^.[, ]+)", "$1"); 
                return t.Name;
            });
            return (p_object != null ? (p_object as string ?? tts(p_object as Type ?? p_object.GetType())) : tts(p_method.DeclaringType))
                + "." + p_method.Name + ((int)(p_method.MemberType & (MemberTypes.Property | MemberTypes.Event)) == 0 ? "()" : null);
        }
        public static string GetQualifiedMethodName(Delegate p_delegate)
        {
            return (p_delegate == null) ? null : GetQualifiedMethodName(p_delegate.Method, p_delegate.Target ?? p_delegate.Method.DeclaringType, true);
        }
        public static string GetQualifiedMemberName(this MemberInfo p_member, bool p_shortClassName = false)
        {
            if (p_member == null)
                return null;
            return (p_shortClassName ? p_member.DeclaringType.Name : p_member.DeclaringType.ToString()) + "." + p_member.Name;
        }
        /// <summary> Example: "invalid {0}".FmtMbr(_=> p_key) == "invalid p_key".
        /// Note that it works even if p_fieldOrPropertyExpression is not a field/property but a parameter </summary>
        public static string FmtMbr(this string p_fmt, params System.Linq.Expressions.Expression<Func<int, object>>[] p_fieldOrPropertyExpression)
        {
            return FormatInvCult(p_fmt, p_fieldOrPropertyExpression.Select(expr => {
                // p_fieldOrPropertyExpression() returns 'object' so   ↓↓ conversion is usually involved
                Expression mex = (expr.Body.NodeType == ExpressionType.Convert) ? ((UnaryExpression)expr.Body).Operand : expr.Body;
                MemberInfo m = ((MemberExpression)mex).Member;
                string name = GetQualifiedMemberName(m, p_shortClassName: true);
                return (name != null && name.StartsWith("<>")) ? m.Name : (object)name;
            }).ToArray());
        }
        #endregion

        ///// <summary> Example: int i = Utils.GetSettingFromExeConfig2(_ => g_MaxLenOfLoggedSqlCmd, 1024);<para>
        ///// Harnesses the name of the field or property ('g_MaxLenOfLoggedSqlCmd' in the example),
        ///// and uses it as key in the &lt;appSettings&gt; section of .exe.config. (The "g_" or "m_"
        ///// prefix of the name is removed if found. In the example "MaxLenOfLoggedSqlCmd" is used.)</para>
        ///// Parses the specified setting as T and returns its value. If unsuccessful (e.g. the setting
        ///// is missing or parsing fails), p_default is returned. <para>
        ///// The value of the specified field/property is left unchanged.
        ///// </para></summary>
        public static T GetSettingFromExeConfig2<T,T1>(Expression<Func<int,T1>> p_fieldOrPropertyExpression, T p_default,
            Func<T, bool> p_validator = null)
        {
            string settingName = Utils.GetNameOf(p_fieldOrPropertyExpression);
            if (settingName.StartsWith("g_") || settingName.StartsWith("m_"))
                settingName = settingName.Substring(2);
            return GetSettingFromExeConfig(settingName).As<T>(p_default, p_validator);
        }
        //public static T GetSettingFromExeConfig<T>(string p_settingName, T p_default, Func<T, bool> p_validator = null)
        //{
        //    return GetSettingFromExeConfig(p_settingName).As<T>(p_default, p_validator);
        //}
        public static Parseable GetSettingFromExeConfig(string p_settingName)
        {
            return Utils.Get(Utils.ExeConfig, p_settingName);
        }
/*
        /// <summary> Helper class that caches a global value of type T, by (default) parsing
        /// the .exe.config file &lt;appSettings Key="..." Value="..."&gt; (see the 'T Get()'
        /// method). Use this when parsing that single appSettings value requires complex
        /// calculation (either the parsing or the calculation of the default value is costly).
        /// TName specifies the "Key" (== typeof(TName).Name =: m_settingName in Init()),
        /// and is also used to make the cached static variable distinct from other
        /// &lt;appSettings&gt; Keys (TName must derive from SettingFromExeConfig&lt;&gt;
        /// and should have no descendants, because those share the same global variable).</summary>
        public class SettingFromExeConfig<T,TName> : DisposablePattern
            where TName : SettingFromExeConfig<T,TName>, new()
        {
            protected T m_value;                    // default value or temporary variable in Get()
            protected string m_settingName;
            protected UpdatableSettings.Slot<object> m_updater;
            public static T Get(T p_default)
            {
                return ((TName)(new TName().Init(p_default))).ProduceValue();
            }
            protected virtual object Init(T p_default)
            {
                m_value = p_default;
                m_settingName = typeof(TName).Name;
                return this;
            }
            protected override void Dispose(bool p_notFromFinalize) { Utils.DisposeAndNull(ref m_updater); }

            protected virtual T ProduceValue()
            {
                object value = Utils.ExeConfig[m_settingName];
                if (value is T)
                    return (T)value;                        // fast path
                // IMPORTANT: you never get here when Utils.ExeConfig[] returns T (e.g. string).
                T result = Parse(new Parseable(value, m_settingName, Utils.ExeConfig));
                Utils.ExeConfig[m_settingName] = result;    // write back. This serves two purposes: 1) to cache & activate the fast path above;
                return result;                              // 2) ensure that assingment of a new value run-time (of non-T type, typically string) will trigger new parsing on next read, and overwrite the cached value
            }
            /// <summary> Not called when T is string -- see the IMPORTANT comment in ProduceValue() </summary>
            protected virtual T Parse(Parseable p_exeCfgVal) { return p_exeCfgVal.As<T>(m_value, IsValid); }
            protected virtual bool IsValid(T p_value) { return true; }
        }
*/
        /// <summary> Use this function to force re-reading the (whole) .exe.config file </summary>
        public static void ResetConfigMechanism(string p_newExeConfigPath = null)
        {
            if (!String.IsNullOrEmpty(p_newExeConfigPath) && System.IO.File.Exists(p_newExeConfigPath))
                AppDomain.CurrentDomain.SetData("APP_CONFIG_FILE", p_newExeConfigPath);

            // I hate binding to undocumented variables, but ConfigurationManager.RefreshSection("appSettings") does nothing.
            // The following code is taken from stackoverflow.com/a/6151688
            Func<Type,string,FieldInfo> getSField = (type,fieldName) =>
                type.GetField(fieldName, BindingFlags.NonPublic |  BindingFlags.Static);

            Type tCM = typeof(System.Configuration.ConfigurationManager);
            getSField(tCM, "s_initState"   ).SetValue(null, 0);
            getSField(tCM, "s_configSystem").SetValue(null, null);
            getSField(tCM.Assembly.GetType("System.Configuration.ClientConfigPaths"), "s_current").SetValue(null, null);
        }

        public static bool IsDebugBuild
        {
            get
            {
                #if DEBUG
                return true;
                #else
                return false;
                #endif
            }
        }

        /// <summary> Reads the value of "IsDebugMode" from p_dict[], or from Utils.ExeConfig[]. Defaults to Utils.IsDebugBuild </summary>
        public static bool IsDebugMode(System.Collections.IDictionary p_dict = null)
        {
            try
            {
                object val;
                Utils.TryGetValuEx(p_dict, IsDebugModeSetting, out val);
                return (val != null) ? Utils.ParseBool(val) : Utils.ExeConfig.Get(IsDebugModeSetting).Default(Utils.IsDebugBuild);
            }
            catch { return Utils.IsDebugBuild; }
        }
        public const string IsDebugModeSetting = "IsDebugMode";

        /// <summary> Returns value of [assembly: AssemblyConfiguration("...")] as specified in Properties/AssemblyInfo.cs </summary>
        public static string GetBuildInfo()
        {
            try
            {
                string s = null; int i = 0;
                do
                {
                    Assembly asm = (i == 0) ? typeof(HQCommon.Utils).Assembly : Assembly.GetEntryAssembly();
                    s = asm.GetCustomAttributes(typeof(AssemblyConfigurationAttribute), false)
                            .Cast<AssemblyConfigurationAttribute>().Select(attr => attr.Configuration).FirstOrDefault();
                } while (String.IsNullOrEmpty(s) && ++i < 2);
                return (s + (Utils.IsDebugBuild ? " Debug" : " Release")).TrimStart();
            } catch {
                return null;
            }
        }

        #region GetGenericTypeArgs, IsGenericImpl
        /// <summary> Calls Utils.GetGenericTypeArgs(p_type,p_generic) and returns
        /// its result in p_actualTypeArgs. Returns false if p_actualTypeArgs==null
        /// or p_isMatch(i,p_actualTypeArgs[i],p_requiredTypes[i]) is false for at
        /// least one element of p_requiredTypes[]. <para> IMPORTANT: only the first
        /// p_requiredTypes[].Length items are examined in p_actualTypeArgs[],
        /// that is, only the first implementation of p_generic is examined!</para>
        /// If p_isMatch is null, the default behaviour is to accept the actual
        /// type (==return true) if it is descendant of the required type, or if
        /// the required type is null.
        /// Note that this accepts a value-type even if the required type is a
        /// reference type (like typeof(object) or some interface type), despite
        /// the fact that this case may require special handling at the caller.
        /// </summary>
        public static bool IsGenericImpl(Type p_type, Type p_generic, out Type[] p_actualTypeArgs,
            Func<int, Type, Type, bool> p_isMatch, params Type[] p_requiredTypes)
        {
            p_actualTypeArgs = GetGenericTypeArgs(p_type, p_generic);
            if (p_actualTypeArgs.Length == 0)
                return false;

            if (p_isMatch == null && 0 < p_requiredTypes.Length)
                p_isMatch = (p_idx, p_actual, p_wanted) =>
                    p_wanted == null || (p_actual != null && p_wanted.IsAssignableFrom(p_actual));
            //p_wanted == null || Equals(p_actual, p_wanted)
            //|| (p_wanted.IsAssignableFrom(p_actual) && !p_actual.IsValueType);

            for (int i = 0; i < p_requiredTypes.Length; ++i)
                if (!p_isMatch(i, i < p_actualTypeArgs.Length ? p_actualTypeArgs[i] : null,
                                p_requiredTypes[i]))
                    return false;
            return true;
        }

        /// <summary> Returns a list of types like {A1,...,B1,...} when p_aType implements
        /// p_genericBase&lt;A1,...&gt; and p_genericBase&lt;B1,...&gt; etc.
        /// Returns Type.EmptyTypes if p_aType does not implement p_genericBase&lt;&gt; at all.
        /// (Never returns null). p_genericBase may be either a generic type definition 
        /// (e.g. typeof(List&lt;&gt;), typeof(ICollection&lt;&gt;))
        /// or a closed generic type (e.g. typeof(List&lt;int&gt;),
        /// typeof(ICollection&lt;int&gt;)), in this latter case the
        /// type argument(s) is/are ignored (only generic def is considered).
        /// If it is an interface, the method searches amongst p_type's interfaces,
        /// otherwise it searches amongst p_type's base classes.<para>
        /// The reflected information is cached so it is much faster next time.
        /// Example:</para>
        ///   Type t = GetGenericTypeArgs(obj.GetType(), typeof(ICollection&lt;&gt;)).FirstOrDefault();
        /// </summary>
        public static Type[] GetGenericTypeArgs(Type p_aType, Type p_genericBase, bool p_cacheResult = true)
        {
            if (!p_genericBase.IsGenericType)
                throw new ArgumentException();
            if (!p_genericBase.IsGenericTypeDefinition)
                p_genericBase = p_genericBase.GetGenericTypeDefinition();
            KeyValuePair<Type, Type[]>[] bases = p_cacheResult ? StaticCacheOfGenericTypeArgs.Get(p_aType)
                : new StaticCacheOfGenericTypeArgs().CalculateValue(p_aType, null);
            for (int i = (bases == null) ? 0 : bases.Length; --i >= 0; )
                if (bases[i].Key == p_genericBase)
                    return bases[i].Value;
            return Type.EmptyTypes;
        }

        class StaticCacheOfGenericTypeArgs : StaticDict<Type, KeyValuePair<Type, Type[]>[], StaticCacheOfGenericTypeArgs>
        {
            public override KeyValuePair<Type, Type[]>[] CalculateValue(Type p_aType, object p_arg)
            {
                var all = new Dictionary<Type, Type[]>();
                p_aType.FindInterfaces(Collect, all);               // collect interfaces
                for (Type t = p_aType; t != null; t = t.BaseType)   // collect bases + recognize things like KeyValuePair<A,B>
                    Collect(t, all);
                return (all.Count == 0) ? null : all.ToArray();
            }
            static bool Collect(Type p_baseOrIntf, object p_dict)
            {
                if (p_baseOrIntf.IsGenericType)
                {
                    var dict = (Dictionary<Type, Type[]>)p_dict;
                    Type g = p_baseOrIntf.GetGenericTypeDefinition();
                    Type[] a, b = p_baseOrIntf.GetGenericArguments();
                    dict.TryGetValue(g, out a);
                    Array.Resize(ref a, (a == null ? 0 : a.Length) + b.Length);
                    Array.Copy(b, 0, a, a.Length - b.Length, b.Length);
                    dict[g] = a;
                }
                return false;
            }
        }
        
        #endregion

        #region ReplaceGenericParameters
        ///// <summary> Returns a new delegate by replacing the generic 
        ///// type argument of p_template to p_newType and the target to
        ///// p_newTarget.
        ///// p_template must contain exactly 1 generic type argument.
        ///// Example: 
        /////   Func&lt;string&gt; myDelegate;
        /////   CreateDelegateForAnotherGenericParameter(new Func&lt;int&gt;(MyMethod&lt;int&gt;),
        /////     typeof(string), myObject, out myDelegate);
        /////   string s = myDelegate();    // calls myObject.MyMethod&lt;string&gt;()
        ///// </summary><exception cref="ArgumentException">if p_newType does
        ///// not meet the restrictions specified by p_template </exception>
        //public static TResult CreateDelegateForAnotherGenericParameter<TResult>(
        //    Delegate p_template, Type p_newType, object p_newTarget)
        //{
        //    MethodInfo m = (MethodInfo)g_cachedMethodInfo[p_template];
        //    if (m == null)
        //    {
        //        m = p_template.Method.GetGenericMethodDefinition();
        //        lock (g_cachedMethodInfo)
        //            g_cachedMethodInfo[p_template] = m;
        //    }
        //    m = m.MakeGenericMethod(new Type[] { p_newType });
        //    return (TResult)(object)Delegate.CreateDelegate(typeof(TResult), p_newTarget, m);
        //}
        //static readonly System.Collections.Hashtable g_cachedMethodInfo 
        //    = new System.Collections.Hashtable();


        /// <summary> Creates a new delegate by replacing the type argument
        /// of p_template with T1 (or the first 2 type arguments of p_template
        /// with T1,T2).
        /// If p_requiredType is not null and T1 does not implement it, a
        /// throw-only delegate is returned instead of using p_template.
        /// The result is recorded in p_cacheForResult to speed up subsequent
        /// calls (the method merely returns p_cacheForResult if it is non-null
        /// at the beginning).
        /// </summary>
        //public static Func<T1, T2> ReplaceFuncParameters<T1, T2>(Delegate p_template,
        //    ref Func<T1, T2> p_cacheForResult, Type p_requiredType)
        //{
        //    if (p_cacheForResult == null)
        //    {
        //        if (p_requiredType.IsAssignableFrom(typeof(T1)))
        //            ReplaceGenericParameters(p_template, out p_cacheForResult,
        //                typeof(T1), typeof(T2));
        //        else
        //            p_cacheForResult = delegate {
        //                throw new NotImplementedException(String.Format(
        //                    "{0} does not implement {1}", typeof(T1), p_requiredType));
        //            };
        //    }
        //    return p_cacheForResult;
        //}

        /// <summary> Creates a new delegate by replacing the first type
        /// arguments of p_template with the types in p_replacementTypes[].
        /// </summary>
        public static TResultDelegate ReplaceGenericParameters<TResultDelegate>(
            Delegate p_template, out TResultDelegate p_result,
            params Type[] p_replacementTypes)
        {
            p_result = (TResultDelegate)(object)ReplaceGenericParameters(p_template,
                typeof(TResultDelegate), p_replacementTypes);
            return p_result;
        }

        public static Delegate ReplaceGenericParameters(Delegate p_template,
            Type p_resultDelegateType, params Type[] p_replacementTypes)
        {
            MethodInfo m = p_template.Method;
            object target = m.IsStatic ? null : p_template.Target;
            if (m.IsGenericMethod)
            {
                if (p_replacementTypes == null)
                    return null;
                Type[] templateArgs = m.GetGenericArguments();
                Array.Copy(p_replacementTypes, templateArgs,
                    Math.Min(p_replacementTypes.Length, templateArgs.Length));
                m = m.GetGenericMethodDefinition();
                m = m.MakeGenericMethod(templateArgs);
            }
            return Delegate.CreateDelegate(p_resultDelegateType, target, m);
        }
        #endregion

        //public static TDelegate ConvertDelegateTo<TDelegate>(Delegate p_delegate) where TDelegate : class
        //{
        //    if (p_delegate == null)
        //        return default(TDelegate);
        //    return (p_delegate as TDelegate) ??
        //        (TDelegate)(object)Delegate.CreateDelegate(typeof(TDelegate), p_delegate.Target, p_delegate.Method);
        //}
        //
        //public static TDelegate ConvertDelegate<TDelegate>(Delegate p_delegate, out TDelegate p_dst) where TDelegate : class
        //{
        //    return p_dst = ConvertDelegateTo<TDelegate>(p_delegate);
        //}

        /// <summary> Returns null if TAttr is not defined on p_memberInfoOrType </summary>
        public static TAttr GetAttribute<TAttr>(MemberInfo p_memberInfoOrType, bool p_inherit = true)
            where TAttr : Attribute
        {
            object[] a = (p_memberInfoOrType == null) ? null : p_memberInfoOrType.GetCustomAttributes(typeof(TAttr), p_inherit);
            return (a == null || a.Length == 0) ? null : a[a.Length - 1] as TAttr;
        }

        #region FindTypeInAllAssemblies
        /// <summary> Considers only assemblies whose FullName is matched by p_asmNameRegexp
        /// (which is a regexp either in a string or in a System.Text.RegularExpressions.Regex). 
        /// This can be used to speed up the search. p_asmNameRegexp==null causes searching 
        /// in all assemblies. Note: assembly FullName looks like these:
        /// "mscorlib, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
        /// "ConsoleApplication1, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
        /// </summary>
        public static Type FindTypeInAllAssemblies(string p_fullClassName, object p_asmNameRegexp = null)
        {
            return FindTypeInAllAssembliesStaticCache.Get(p_fullClassName, p_asmNameRegexp);
        }
        class FindTypeInAllAssembliesStaticCache : StaticDict<string, Type, FindTypeInAllAssembliesStaticCache>
        {
            public override Type CalculateValue(string p_fullClassName, object p_asmNameRegexp)
            {
                var filter = p_asmNameRegexp as System.Text.RegularExpressions.Regex;
                if (p_asmNameRegexp != null && filter == null)
                    filter = new System.Text.RegularExpressions.Regex(p_asmNameRegexp.ToString());
                Type[] typeArgs = ExtractGenericTypeArguments(ref p_fullClassName, ref filter);
                Type result = null;
                if (!String.IsNullOrEmpty(p_fullClassName))
                    foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                    {
                        if (filter != null && !filter.IsMatch(assembly.FullName))
                            continue;
                        result = assembly.GetType(p_fullClassName);
                        if (result != null)
                        {
                            result = typeArgs != null ? result.MakeGenericType(typeArgs) : result;
                            break;
                        }
                    }
                return result;
            }
            /// <summary>
            /// Examples: 
            /// Input: p_fullClassName == "System.Collections.Generic.List`1[System.Int32]"
            ///        (produced by Type.ToString())
            /// Calls FindTypeInAllAssemblies() recursively to resolve generic type arguments.
            /// Returns new Type[1] { System.Int32 }
            ///        p_fullClassName:= "System.Collections.Generic.List`1"
            ///        p_asmNameRegexp unchanged.
            ///
            /// Input: p_fullClassName == "System.Int32"
            /// Does nothing.
            /// Returns null
            ///        p_fullClassName, p_asmNameRegexp unchanged.
            /// 
            /// Input: p_fullClassName == "System.Int32, mscorlib, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
            /// Returns null
            ///        p_fullClassName:="System.Int32", 
            ///        p_asmNameRegexp:="mscorlib, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
            /// </summary>
            static Type[] ExtractGenericTypeArguments(ref string p_fullClassName,
                ref System.Text.RegularExpressions.Regex p_asmNameRegexp)
            {
                Type[] result = null;
                int i = p_fullClassName.IndexOf('`');
                do
                {
                    if (i < 0)
                        break;
                    int j = p_fullClassName.IndexOf('[', i);
                    if (j < 0)
                        break;
                    int k = FindMatchingPair('[', ']', p_fullClassName, j);
                    if (k < 0)
                        break;
                    var typeArgs = new List<Type>();
                    for (int i2 = j + 1, i3 = i2; i3 <= k; ++i3)
                    {
                        char ch = p_fullClassName[i3];
                        if (ch == '[')
                        {
                            bool atBegin = (i3 == i2);
                            i3 = FindMatchingPair('[', ']', p_fullClassName, i3);
                            if (i3 < 0)
                                i3 = k;
                            if (atBegin)
                            {
                                i2 += 1;
                                ch = ',';
                            }
                        }
                        if (ch == ',' || i3 >= k)
                        {
                            if (i3 > i2)
                            {
                                Type t = FindTypeInAllAssemblies(p_fullClassName.Substring(i2, i3 - i2),
                                    p_asmNameRegexp);
                                if (t == null)
                                {
                                    p_fullClassName = null;
                                    return null;
                                }
                                typeArgs.Add(t);
                            }
                            i2 = i3 + 1;
                        }
                    }
                    p_fullClassName = p_fullClassName.Remove(j, k - j + 1);
                    result = typeArgs.ToArray();
                } while (false);
                i = p_fullClassName.IndexOf(',');
                if (i >= 0)         // it's an assembly-qualified name
                {
                    string asm = p_fullClassName.Substring(i + 1).TrimStart();
                    p_asmNameRegexp = new System.Text.RegularExpressions.Regex(asm);
                    p_fullClassName = p_fullClassName.Substring(0, i);
                }
                return result;
            }
        } //~ FindTypeInAllAssembliesStaticCache
        #endregion

        public static IEnumerable<Type> FindImplementations(this Type p_intfOrBaseClass, IEnumerable<Type> p_types)
        {
            if (p_intfOrBaseClass == null || p_types == null)
                return Type.EmptyTypes;

            var isImpl = !p_intfOrBaseClass.IsGenericTypeDefinition ? new Func<Type, bool>(p_intfOrBaseClass.IsAssignableFrom)
                : t => Utils.GetGenericTypeArgs(t, p_intfOrBaseClass, p_cacheResult: false).Length != 0;

            return p_types.Where(t => !t.IsInterface && !t.IsAbstract && isImpl(t)
                && !t.IsDefined(typeof(System.CodeDom.Compiler.GeneratedCodeAttribute), inherit: false));
        }
        /// <summary> p_intfOrBaseClass==null means typeof(Object), which will filter out interface/abstract/[GeneratedCode] types </summary>
        public static IEnumerable<Type> FindImplementations(this IEnumerable<Type> p_types, Type p_intfOrBaseClass = null)
        {
            return FindImplementations(p_intfOrBaseClass ?? typeof(object), p_types);
        }
        /// <summary> bool?==null specifies to omit that assembly, false means assembly.GetExportedTypes(),
        /// true means assembly.GetTypes(). </summary>
        public static IEnumerable<Type> EnumerateAllTypes(this IEnumerable<KeyValuePair<Assembly, bool?>> p_categorizedAssemblies)
        {
            foreach (KeyValuePair<Assembly, bool?> kv in p_categorizedAssemblies.EmptyIfNull())
            {
                if (kv.Key != null && kv.Value != null)
                    foreach (Type t in (kv.Value.Value ? kv.Key.GetTypes() : kv.Key.GetExportedTypes()))
                        yield return t;
            }
        }

        /// <summary> Not thread-safe! Filters out those items from p_allAssemblies[] that are present in 
        /// p_alreadyEnumeratedAsm[], too; and adds the returned items to p_alreadyEnumeratedAsm[].
        /// p_alreadyEnumeratedAsm[] stores WeakReferences to allow unloading assemblies on-the-fly. </summary>
        public static IEnumerable<Assembly> DoNotEnumerateSameAssemblyAgain(this IEnumerable<Assembly> p_allAssemblies,
            ref List<WeakReference> p_alreadyEnumeratedAsm)
        {
            if (p_alreadyEnumeratedAsm == null)
                p_alreadyEnumeratedAsm = new List<WeakReference>();
            var result = new List<Assembly>();
            foreach (Assembly asm in p_allAssemblies)
            {   // O(n^2) algorithm but affordable because n:=nr.of assemblies < 1000, and executed rarely
                int i = p_alreadyEnumeratedAsm.Count;
                while (--i >= 0)
                {
                    var done = p_alreadyEnumeratedAsm[i].Target as Assembly;
                    if (done == asm)
                        break;
                    if (done == null)
                        Utils.FastRemoveAt(p_alreadyEnumeratedAsm, i);
                }
                if (i < 0)
                    result.Add(asm);
            }
            p_alreadyEnumeratedAsm.AddRange(result.Select(asm => new WeakReference(asm)));
            return result;
        }

        /// <summary> Associates bool? values to Assemblies as understood by EnumerateAllTypes():
        /// null if the assembly should be omitted, false = assembly.GetExportedTypes(), true = assembly.GetTypes().<para>
        /// p_userAll      specifies the behaviour for user-assemblies (for which IsUserAssembly()==true)</para>
        /// p_frameworkAll specifies the behaviour for framework-assemblies (all other assemblies).
        /// </summary> 
        public static IEnumerable<KeyValuePair<Assembly, bool?>> CategorizeLoadedAssemblies(
            bool? p_userAll, bool? p_frameworkAll, IEnumerable<Assembly> p_seq = null)
        {
            if (p_seq == null)
                p_seq = AppDomain.CurrentDomain.GetAssemblies();
            if (p_userAll == p_frameworkAll)
                return Utils.MakePairs(p_seq, p_userAll);
            return p_seq.Select(asm => new KeyValuePair<Assembly, bool?>(asm, IsUserAssembly(asm) ? p_userAll : p_frameworkAll));
        }
        public static IEnumerable<KeyValuePair<Assembly, bool?>> CategorizeLoadedAssemblies(
            this IEnumerable<Assembly> p_seq, bool? p_userAll, bool? p_frameworkAll)
        {
            return CategorizeLoadedAssemblies(p_userAll, p_frameworkAll, p_seq);
        }
        /// <summary> See also ServiceStack.StringExtensions.IsUserType(Type) </summary>
        public static bool IsUserAssembly(Assembly p_asm)
        {
            if (p_asm == null)
                return false;
            // Note that the following is not a definition but a heuristic, which currently works and is to be refined in the future.
            AssemblyName name = p_asm.GetName();
            return (name.GetPublicKeyToken().Length == 0) && !name.Name.StartsWith("ServiceStack");
        }

        public static IEnumerable<KeyValuePair<Type, Exception>> InstantiateAndInvokeAllImplementationsOf<T>(Action<T> p_invoker,
            Func<Type, object> p_factory = null, bool? p_userAll = true, bool? p_frameworkAll = null,
            Func<IEnumerable<KeyValuePair<Assembly, bool?>>,IEnumerable<KeyValuePair<Assembly, bool?>>> p_assemblyFilter = null)
        {
            var assemblies = CategorizeLoadedAssemblies(p_userAll, p_frameworkAll);
            if (p_assemblyFilter != null)
                assemblies = p_assemblyFilter(assemblies);
            foreach (Type type in assemblies.EnumerateAllTypes().FindImplementations(typeof(T)))
            {
                Exception e = null;
                try
                {
                    T t = (T)((p_factory == null ? null : p_factory(type)) ??
                        type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
                    if (t != null)
                        p_invoker(t);
                }
                catch (Exception ee) { e = ee; }
                if (e != null)
                    yield return new KeyValuePair<Type, Exception>(type, e);
            }
        }


        // "HQCommon.DBUtils+<>c__DisplayClass51"
        // -> HQCommon.DBUtils._c__DisplayClass51
        // "HQCommon.ListLookupDictionary`2+<System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<TKey\,TValue>>.GetEnumerator>d__13[TKey,TValue]"
        // -> HQCommon.ListLookupDictionary.System.Collections.Generic.IEnumerable.System.Collections.Generic.KeyValuePair.TKey.TValue.GetEnumerator_d__13
        // "HQCommon.AccessOrderCache`2+GlobalTable+PendingSet+<System.Collections.Generic.IEnumerable<System.Int32>.GetEnumerator>d__49[TArgument,TValue]"
        // -> HQCommon.AccessOrderCache.GlobalTable.PendingSet.System.Collections.Generic.IEnumerable.System.Int32.GetEnumerator_d__49
        /// <summary> Example: "HQCommon.ListLookupDictionary`2+DataStructure+ᐸGetDistinctKeysᐳd__2b[TKey,TValue]"
        /// → HQCommon.ListLookupDictionary.DataStructure.GetDistinctKeys_d__2b </summary>
        public static string GetTidyTypeName(Type p_type, bool p_fullName = false)
        {
            string result = (p_type == null) ? String.Empty : (p_fullName ? p_type.ToString() : p_type.Name);
            var sb = new System.Text.StringBuilder(result.Length);
            for (int i = 0, j = 1; i < result.Length; ++i)
                switch (result[i])
                {
                    case '[': return sb.ToString();
                    case '`': j = 0; break;
                    case '\\': break;
                    case '<':
                        if (j != 0 && 0 < i && result[i - 1] != '+')
                            sb.Append('.');
                        break;
                    case '+': 
                    case ',': j = 1; sb.Append('.'); break;
                    case '>':
                        if (i + 1 < result.Length && result[i + 1] != '.' && result[i + 1] != '>')
                            sb.Append('_');
                        break;
                    default: if (j != 0) sb.Append(result[i]); break;
                }
            return sb.ToString();
        }
    }

    public delegate T2 FuncWithRefArg<T1, T2>(ref T1 p_arg);

    /// <summary>Usage:<para>
    ///   MemberInfo[] members = PublicFieldsPropertiesStaticCache.Get(Type);
    /// </para></summary>
    public class PublicFieldsPropertiesStaticCache : StaticDict<Type, MemberInfo[], PublicFieldsPropertiesStaticCache>
    {
        public override MemberInfo[] CalculateValue(Type p_type, object p_arg)
        {
            var result = new List<MemberInfo>();
            BindingFlags flags = (p_arg as BindingFlags?).GetValueOrDefault();
            flags |= BindingFlags.Instance | BindingFlags.Public | BindingFlags.FlattenHierarchy;
            result.AddRange(p_type.GetProperties(flags));
            result.AddRange(p_type.GetFields(flags));
            return result.ToArray();
        }
    }

    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property | AttributeTargets.Method | AttributeTargets.Constructor | AttributeTargets.Parameter,
        AllowMultiple=false, Inherited=true)]
    public class HQInjectAttribute : Attribute
    {
        public readonly object Param0;
        public HQInjectAttribute() { }
        public HQInjectAttribute(object p_param) { Param0 = p_param; }

        /// <summary> Enumerates all fields/properties that have the [HQInject] attribute
        /// (including non-public members). For static properties/fields, the current value
        /// is checked first and if not null, left untouched.<para>
        /// IMPORTANT: field/property injection is evil (see http://goo.gl/W6hDTo), therefore
        /// use this method with caution, as a workaround only (e.g. DAC_ControllerBase
        /// contains an explanation) </para></summary>
        public static T InjectIntoPropertiesAndFields<T>(T p_obj, Func<Type, object> p_tryResolve)
        {
            foreach (var m in HQInjectAttribute.ListOfMembersToInject.Get(p_obj.GetType()))
            {
                if (Utils.IsStaticMember(m) && Utils.GetValueOfMember(m, p_obj) != null)
                    continue;
                object value = p_tryResolve(Utils.GetTypeOfMember(m));
                if (value != null)
                    Utils.SetValueOfMember(m, p_obj, value);
            }
            return p_obj;
        }

        public class ListOfMembersToInject : StaticDict<Type, MemberInfo[], ListOfMembersToInject>
        {
            public override MemberInfo[] CalculateValue(Type p_type, object p_arg)
            {
                return new PublicFieldsPropertiesStaticCache().CalculateValue(p_type, BindingFlags.Static | BindingFlags.NonPublic)
                    .Where(m => m.IsDefined(typeof(HQInjectAttribute), true)).ToArray();
            }
        }
    }


/*
    /// <summary> Usage:
    /// <para> MethodInfo parse   = (MethodOfᐸFuncᐸstring, intᐳᐳ)int.Parse; </para>
    /// <para> MethodInfo writeln = (MethodOfᐸActionᐳ)Console.WriteLine; </para>
    /// </summary><remarks>
    /// Credits: http://stackoverflow.com/questions/1213862/
    /// </remarks>
    public struct MethodOf<TDelegate>
    {
        public readonly MethodInfo Info;
        public MethodOf(TDelegate d)
        {
            Info = ((Delegate)(object)d).Method;
        }
        public static implicit operator MethodInfo(MethodOf<TDelegate> p_this)
        {
            return p_this.Info;
        }
        public static implicit operator MethodOf<TDelegate>(TDelegate p_delegate)
        {
            return new MethodOf<TDelegate>(p_delegate);
        }
    }
 */
}
