//#define SupportAttributeRegistrations
using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
using System.Reflection;
using System.Linq.Expressions;

namespace HQCommon
{
#if UPDATABLE_SETTINGS
    /// <summary> UpdatableSettings.Register() creates Slots to store callbacks for setting names,
    /// to be run when the value of the named setting changes. Changes can be signaled programmatically
    /// (Slot.Update() or UpdateSetting("key","value")), or can be auto-detected by monitoring the
    /// _current_ AppSettings instance regularly. Both involves comparing the cached Slot.Value
    /// of the registered settings with their current value from AppSettings.
    /// The goal is to update custom data structures (or clear caches) that are somehow associated with
    /// a setting. <para>For example, setting values (= strings) can be automatically parsed/reflected
    /// into typed static fields/properties (this is what DiscoverAttributeRegistrations() can do).
    /// 
    /// When UpdatableSettings.Register() is invoked for the first time about a setting, the specified
    /// callback gets notified immediately, as part of the registration.
    /// 
    /// Monitoring starts when 1) the MonitoringFreq property is assigned with a positive value,
    /// or 2) when a registration specifies MonitorIndividually=true (this starts monitoring
    /// of that registration only).
    /// </para>
    /// Note that DiscoverAttributeRegistrations() currently does nothing, because #SupportAttributeRegistrations
    /// is undefined. When #defined, DiscoverAttributeRegistrations() enumerates all (new) loaded assemblies
    /// and finds classes marked with [HasUpdatableGlobalSettings], then in these classes creates registrations
    /// for all static fields/properties marked with [UpdatableSetting].
    /// (Each assembly is enumerated at most once, even if DiscoverAttributeRegistrations() is called multiple times.)
    /// </summary>
    public class UpdatableSettings : DisposablePattern
    {
        List<Slot> m_slotsUsingSharedTimer = new List<Slot>();
        HqTimer m_sharedTimer;
        TimeSpan m_monitoringFreq, m_wait;
        IObjSettings m_settingStore;

        // 'internal' because accessed from HQAppBase.Exit0()
        internal static UpdatableSettings g_instance;
        public static Func<UpdatableSettings> g_singletonFactory = () => new UpdatableSettings();

        public static UpdatableSettings Singleton
        {
            get
            {
                return Utils.ThreadSafeLazyInit(ref g_instance, false, typeof(UpdatableSettings), 0, _ => g_singletonFactory());
            }
        }

        [HQInject]
        public IObjSettings AppSettings
        {
            get { return m_settingStore ?? Utils.ExeConfig; }
            set { m_settingStore = value; }
        }

        public TimeSpan MonitoringFreq
        {
            get { return m_monitoringFreq; }
            set
            {
                bool same = (m_monitoringFreq == value);
                m_monitoringFreq = m_wait = value;
                if (same)
                    UpdateAllNow();
                else if (m_monitoringFreq.Ticks <= 0)
                    Utils.DisposeAndNull(ref m_sharedTimer);
                else
                    HqTimer.Start(ref m_sharedTimer, () => m_wait, this, p_this => p_this.MonitorUpdates());
            }
        }

        public UpdatableSettings() { }
        public UpdatableSettings(IObjSettings p_settingStore)
        {
            AppSettings = p_settingStore;
        }

        public abstract class RegBase
        {
            internal Func<object,object> m_getter;
            internal Action<object,object> m_setter;
            internal object m_defValue;
            #pragma warning disable 0649    // warning CS0649: Field '...' is never assigned to, and will always have its default value 0
            internal Action<Slot, object> m_eventHandler;   // arguments: (slot, newValue)
            #pragma warning restore 0649

            /// <summary> Not used if CustomUpdaterFunc is set </summary>
            public string SettingName;
            public TimeSpan CacheTimeout;
            /// <summary> true: use individual Timer to monitor this setting. Ignored if CacheTimeout ≤ 0;
            /// false (default): use the shared timer of UpdatableSettings. </summary>
            public bool MonitorIndividually;

            internal abstract void RegisterInto(UpdatableSettings p_owner);
        }
        public class Registration<T> : RegBase
        {
            /// <summary> Receives the old/default value as input, and modifies it to a
            /// new value (+return true) or affirms it (+ return false) </summary>
            public delegate bool ValueFactoryDelegate(ref T p_current);

            public ValueFactoryDelegate CustomUpdaterFunc;

            public T DefaultValue {  get { return Convert(m_defValue); } set { m_defValue = value; } }

            /// <summary> Not used if CustomUpdaterFunc is set </summary>
            public Expression<Func<T>> Member;
            /// <summary> Defaults to EqualityComparer≺T≻.Default.Equals(). Not used if CustomUpdaterFunc is set. </summary>
            public Func<T, T, bool> CompareOldNew;

            internal override void RegisterInto(UpdatableSettings p_owner) { p_owner.Register(this); }
            internal T Convert(object p_defaultValue)
            {
                return new Parseable(p_defaultValue, SettingName, null).As<T>();
            }
        }

        /// <summary> See the comments at the UpdatableSettings class </summary>
        public static Slot<T> Register<T>(ref Slot<T> p_slot, string p_settingName, T p_defValue,
                Expression<Func<T>> p_fieldOrPropertyExpr = null, int p_cacheTimeoutMs = 15000,
                Func<T, T, bool> p_compareOldNew = null, Action<Slot<T>, T> p_changedDelegate = null,
                bool p_registerIfExists = false, bool p_runChgDelegate = false)
        {
            if (p_slot == null || p_registerIfExists)
                Utils.ThreadSafeLazyInit(ref p_slot, p_registerIfExists, Singleton, 0, anInt => {
                    var slot = UpdatableSettings.Singleton.Register<T>(p_settingName, p_defValue,
                    p_fieldOrPropertyExpr, p_cacheTimeoutMs, p_compareOldNew, p_registerIfExists);
                    if (p_changedDelegate != null) slot.ChangedDelg = p_changedDelegate;
                    if (p_runChgDelegate) slot.OnChanged();
                    return slot;
                });
            return p_slot;
        }

        /// <summary> p_fieldOrPropertyExpr, if not null, must be a MemberExpression referring to a static field or property.
        /// That field/property will be updated when the returned Slot.CachedValue changes. If p_fieldOrPropertyExpr==null,
        /// the current value can be queried from Slot.CachedValue only. </summary>
        public Slot<T> Register<T>(string p_settingName, T p_defValue, 
            Expression<Func<T>> p_fieldOrPropertyExpr = null, int p_cacheTimeoutMs = 15000,
            Func<T, T, bool> p_compareOldNew = null, bool p_registerIfExists = false)
        {
            if (!p_registerIfExists)
            {
                Slot<T> result = FindSlots(p_fieldOrPropertyExpr, p_settingName).FirstOrDefault();
                if (result != null)
                    return result;
            }
            var r = new Registration<T> {
                SettingName = p_settingName,
                Member = p_fieldOrPropertyExpr,
                CompareOldNew = p_compareOldNew,
                CacheTimeout = TimeSpan.FromMilliseconds(p_cacheTimeoutMs)
            };
            if (p_defValue != null)
                r.DefaultValue = p_defValue;
            return Register(r);
        }

        public Slot<T> Register<T>(Registration<T>.ValueFactoryDelegate p_updaterFunc,
            T p_default = default(T), int p_cacheTimeoutMs = 15000)
        {
            var r = new Registration<T> {
                CustomUpdaterFunc = p_updaterFunc,
                CacheTimeout = TimeSpan.FromMilliseconds(p_cacheTimeoutMs)
            };
            if (p_default != null)
                r.DefaultValue = p_default;
            return Register(r);
        }

        public Slot<T> Register<T>(Registration<T> p_reg)
        {
            object host = null;
            var slot = new Slot<T> { CacheTimeout = p_reg.CacheTimeout };
            if (p_reg.CustomUpdaterFunc != null)
            {
                Registration<T>.ValueFactoryDelegate userDefined = p_reg.CustomUpdaterFunc;
                slot.ValueFactory = () => userDefined(ref slot.CachedValue);
            }
            else if (p_reg.Member != null)
            {
                MemberInfo mi = ((System.Linq.Expressions.MemberExpression)p_reg.Member.Body).Member;
                SetAccessors(p_reg, mi);
                if (!Utils.IsStaticMember(mi))
                    throw new NotImplementedException("TODO");  // host := extract the instance from p_reg.Member.Body.Expression
            }
            if (slot.ValueFactory == null)
            {
                if (p_reg.m_defValue == null && p_reg.m_getter != null)
                    slot.CachedValue = (T)p_reg.m_getter(host);
                slot.SettingName = p_reg.SettingName;

                /*
                ** Default ValueFactory
                */
                Action<object, object> setter = p_reg.m_setter;
                Func<T, T, bool> eqCmp = p_reg.CompareOldNew;
                var whost = (host == null) ? null : new WeakReference(host);
                slot.ValueFactory = () => {
                    object h = null;
                    if (whost != null && null == (h = whost.Target))
                    {
                        slot.Dispose();
                        return false;
                    }
                    if (this.AppSettings == null)
                    {
                        Utils.Logger.Verbose("AppSettings==null when updating {0} {1}", slot.SettingName, slot.GetType());
                        return false;
                    }
                    T newVal = this.AppSettings.Get<T>(slot.SettingName, slot.CachedValue);
                    bool isSame = (eqCmp != null) ? eqCmp(slot.CachedValue, newVal) : EqualityComparer<T>.Default.Equals(slot.CachedValue, newVal);
                    if (!isSame)
                    {
                        slot.CachedValue = newVal;
                        Utils.Logger.Info("Config change: {0}:={1}", slot.SettingName, 
                            Utils.RemoveMiddleIfLong(slot.CachedValue.ToStringOrNull(), 32));
                        if (setter != null)
                            setter(host, slot.CachedValue);
                    }
                    return !isSame;
                };
            }
            if (p_reg.m_defValue != null)
                slot.CachedValue = p_reg.DefaultValue;
            if (p_reg.m_eventHandler != null)
            {
                Action<Slot, object> eh = p_reg.m_eventHandler;
                slot.ValueChanged += (p_slot,p_newVal) => eh(p_slot, p_newVal);
            }

            slot.Update();

            if (p_reg.MonitorIndividually && TimeSpan.Zero < p_reg.CacheTimeout)
                HqTimer.Start(ref slot.m_timer, () => slot.CacheTimeout, slot, p_slot => p_slot.Update());
            else while (true)
            {
                List<Slot> before = m_slotsUsingSharedTimer, tmp = new List<Slot>(before) { slot };
                if (Interlocked.CompareExchange(ref m_slotsUsingSharedTimer, tmp, before) == before)
                    break;
            }
            return slot;
        }

        void Unregister(Slot p_slot)
        {
            List<Slot> before, tmp;
            do
            {
                before = m_slotsUsingSharedTimer; tmp = null;
                for (int i = (before == null) ? 0 : before.Count; --i >= 0;)
                    if (before[i] == p_slot)
                    {
                        tmp = new List<Slot>(before);
                        tmp.FastRemoveAt(i);
                        break;
                    }
            } while (tmp != null && Interlocked.CompareExchange(ref m_slotsUsingSharedTimer, tmp, before) != before);
        }

        /// <summary> p_fieldOrProperty is used to define T and to look up attributes if p_settingNames[] is empty </summary>
        public IEnumerable<Slot<T>> FindSlots<T>(Expression<Func<T>> p_fieldOrProperty, params string[] p_settingNames)
        {
            Slot<T> st;
            if (p_settingNames != null && p_settingNames.Length == 0)
                p_settingNames = null;
            #if SupportAttributeRegistrations
            if (p_settingNames == null && p_fieldOrProperty != null)
            {
                string[] names = (from attr in ((System.Linq.Expressions.MemberExpression)p_fieldOrProperty.Body)
                                                .Member.GetCustomAttributes<UpdatableSettingAttribute>(false)
                                  where attr.SettingName != null
                                  select attr.SettingName).ToArray();
                if (0 < names.Length)
                    p_settingNames = names;
            }
            #endif
            foreach (Slot s in m_slotsUsingSharedTimer)
                if ((p_settingNames == null || 0 <= Array.IndexOf(p_settingNames, s.SettingName))
                    && null != (st = s as Slot<T>))
                    yield return st;
        }

        public abstract class Slot : DisposablePattern
        {
            public TimeSpan CacheTimeout;
            /// <summary> Updates CachedValue and returns true if the new value is different. </summary>
            public Func<bool> ValueFactory;
            /// <summary> Null for CustomUpdaterFunc registrations </summary>
            public string SettingName;
            internal long m_lastUpdateTicks;
            internal HqTimer m_timer;
            internal UpdatableSettings m_owner;

            protected override void Dispose(bool p_notFromFinalize)
            {
                Utils.DisposeAndNull(ref m_timer);
                UpdatableSettings owner = Interlocked.Exchange(ref m_owner, null);
                if (owner != null)
                    owner.Unregister(this);
            }
            /// <summary> Returns true if the cache timed out AND ValueFactory()==true </summary>
            public bool Update()
            {
                long now = DateTime.UtcNow.Ticks, L = m_lastUpdateTicks;
                if (now - L < CacheTimeout.Ticks
                    || Interlocked.CompareExchange(ref m_lastUpdateTicks, now, L) != L)
                    return false;
                // Note: multiple threads MAY run here IFF ValueFactory() runs longer than CacheTimeout
                //                                     AND this method is called directly (not by Timer)
                if (!ValueFactory())
                    return false;   // no change
                OnChanged();
                return true;
            }
            public abstract void OnChanged();
            public abstract object ObjValue { get; }
        }
        public class Slot<T> : Slot
        {
            public T CachedValue;
            public event Action<Slot<T>, T> ValueChanged;
            public T Value { get { return CachedValue; } }
            public override object ObjValue { get { return CachedValue; } }
            public Action<Slot<T>, T> ChangedDelg { get { return ValueChanged; } set { ValueChanged = value; } }

            public Slot<T> AddChangeHandlerOnce(Action<Slot<T>, T> p_handler)
            {
                if (ValueChanged == null)
                    ValueChanged = p_handler;
                return this;
            }
            protected override void Dispose(bool p_notFromFinalize)
            {
                base.Dispose(p_notFromFinalize);
                if (CachedValue is IDisposable)
                    using (Utils.Swap(ref CachedValue, default(T)) as IDisposable) { }
            }
            public override void OnChanged()
            {
                Action<Slot<T>, T> e = ValueChanged;
                if (e != null)
                    e(this, CachedValue);
            }
            public void Update(T p_newValue)
            {
                if (SettingName != null && m_owner != null)
                {
                    m_owner.AppSettings[SettingName] = p_newValue;  // necessary because UpdateAll() re-parses the value from AppSettings[]
                    m_owner.UpdateAllNow();                         // and thus overwrites our local .CachedValue
                }
                else
                {
                    CachedValue = p_newValue;
                    OnChanged();
                }
            }
        }

        void MonitorUpdates()
        {
            if (AppSettings == null)
            {
                Utils.Logger.Verbose("{0} does nothing because AppSettings==null", Utils.GetCurrentMethodName());
                return;
            }
            if (m_slotsUsingSharedTimer.Count == 0)
                return;
            DateTime next = (DateTime.UtcNow + m_monitoringFreq);
            foreach (Slot s in m_slotsUsingSharedTimer)
                Utils.TryOrLog(s, p_slot => p_slot.Update(), System.Diagnostics.TraceLevel.Error);
            m_wait = next - DateTime.UtcNow;
            if (m_wait < TimeSpan.Zero)
                m_wait = TimeSpan.Zero;
        }
        void UpdateAllNow()
        {
            if (m_sharedTimer != null)
                m_sharedTimer.Change(0);
            else lock(this)
                MonitorUpdates();
        }
        //static void StartTimer<T>(ref Timer p_timer, Func<TimeSpan> p_freq, object p_sync, T p_cbArg, Action<T> p_cb)
        //{
        //    if (p_timer != null || int.MaxValue < p_freq().TotalMilliseconds + 10)
        //        return;
        //    Timer t = null;
        //    lock(p_sync)
        //        if (p_timer == null)
        //            p_timer = t = new Timer(delegate {
        //                p_cb(p_cbArg);
        //                t.Change(10 + (int)p_freq().TotalMilliseconds, -1);
        //            }, null, 10 + (int)p_freq().TotalMilliseconds, -1);
        //}
        protected override void Dispose(bool p_notFromFinalize)
        {
            Utils.DisposeAndNull(ref m_sharedTimer);
            Utils.DisposeAll(Interlocked.Exchange(ref m_slotsUsingSharedTimer, new List<Slot>()));
        }

        /// <summary> Similar to AppSettings[p_key] = p_value, but handles p_key=="Utils.Logger.Level" specially:
        /// it is parsed and written to the appropriate global field immediately, instead of AppSettings.
        /// <para> Furthermore this method calls UpdateAllNow() to evaluate *all* Slots for changes. Note that
        /// the callbacks will be notified for changed Slots only if their CacheTimeout has also expired.</para>
        /// Returns an object with {Key=p_key, New=p_value, Old=...}  properties. </summary>
        public object UpdateSetting(string p_key, string p_value)
        {
            if (String.IsNullOrEmpty(p_key))
                throw new ArgumentException("empty key");

            Utils.Logger.Info("{2}() {0} := {1}", p_key, p_value, MethodInfo.GetCurrentMethod().Name);
            object old;
            if (p_key == "Utils.Logger.Level")
            {
                old = Utils.Logger.Level.ToString();
                System.Diagnostics.TraceLevel @new;
                @new = EnumUtils<System.Diagnostics.TraceLevel>.TryParse(p_value, out @new, true) ? @new : Utils.Logger.Level;
                p_value = (Utils.Logger.Level = @new).ToString();
            }
            else
            {
                old = AppSettings[p_key].ToStringOrNull();
                if (p_value != old.ToStringOrNull())
                {
                    AppSettings[p_key] = p_value;
                    p_value = AppSettings[p_key].ToStringOrNull();
                }
            }
            UpdateAllNow();
            return new { Key = p_key, Old = old, New = p_value };
        }

        static void SetAccessors(RegBase p_reg, MemberInfo p_fieldOrProp)
        {
            if (p_fieldOrProp is FieldInfo)
            {
                p_reg.m_getter = ((FieldInfo)p_fieldOrProp).GetValue;
                p_reg.m_setter = ((FieldInfo)p_fieldOrProp).SetValue;
            }
            else
            {
                p_reg.m_getter = ((PropertyInfo)p_fieldOrProp).GetValue;
                p_reg.m_setter = ((PropertyInfo)p_fieldOrProp).SetValue;
            }
            //ParameterExpression par = Expression.Parameter(typeof(object), "host");
            //ParameterExpression val = Expression.Parameter(typeof(object), "newValue");
            //var valAsT = new Expression[] { Expression.Convert(val, Utils.GetTypeOfMember(p_fieldOrProp)) };
            //if (p_fieldOrProp.MemberType == MemberTypes.Field)
            //{
            //    var f = Expression.Field(((FieldInfo)p_fieldOrProp).IsStatic ? (UnaryExpression)null 
            //        : Expression.Convert(par, p_fieldOrProp.DeclaringType), (FieldInfo)p_fieldOrProp);
            //    p_reg.m_getter = Expression.Lambda<Func<object, object>>(Expression.TypeAs(f, typeof(object)), new[] { par }).Compile();
            //    p_reg.m_setter = Expression.Lambda<Action<object, object>>(Expression.Assign(f, valAsT[0]), new[] { par, val }).Compile();
            //} else {
            //    var p = (PropertyInfo)p_fieldOrProp;
            //    MethodInfo mGet = p.GetGetMethod(true);
            //    Expression host = mGet.IsStatic ? (UnaryExpression)null : Expression.Convert(par, p_fieldOrProp.DeclaringType);
            //    p_reg.m_getter = Expression.Lambda<Func<object, object>>(Expression.Convert(Expression.Call(host, mGet), typeof(object)),new[]{par}).Compile();
            //    p_reg.m_setter = Expression.Lambda<Action<object, object>>(Expression.Call(host, p.GetSetMethod(true), valAsT), new[] { par, val }).Compile();
            //}
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public void DiscoverAttributeRegistrations()
        {
#if !SupportAttributeRegistrations
        }
    }
#else
            foreach (Type t in AppDomain.CurrentDomain.GetAssemblies().DoNotEnumerateSameAssemblyAgain(ref g_asmDone)
                .CategorizeLoadedAssemblies(true, false).EnumerateAllTypes().FindImplementations()
                .Where(t => t.IsDefined(typeof(HasUpdatableGlobalSettingsAttribute), false)
                            || typeof(ISettingChangeHandler).IsAssignableFrom(t) ))
            {
                const BindingFlags B = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy;
                bool found = false;
                foreach (var kv in from m in t.GetFields(B).Concat<MemberInfo>(t.GetProperties(B))
                                   let attrs = m.GetCustomAttributes<UpdatableSettingAttribute>(false)
                                   from a in attrs
                                   select new KeyValuePair<MemberInfo, UpdatableSettingAttribute>(m, a))
                {
                    var reg = (RegBase)Activator.CreateInstance(typeof(Registration<>).MakeGenericType(Utils.GetTypeOfMember(kv.Key)));
                    reg.SettingName = kv.Value.SettingName;
                    reg.CacheTimeout = TimeSpan.FromMilliseconds(kv.Value.CacheTimeoutMs);
                    reg.MonitorIndividually = kv.Value.MonitorIndividually;
                    reg.m_defValue = kv.Value.DefaultValue;
                    if (kv.Value.OnUpdate != null)
                        reg.m_eventHandler = ((ISettingChangeHandler)Activator.CreateInstance(kv.Value.OnUpdate)).OnSettingChange;
                    SetAccessors(reg, kv.Key);
                    if (String.IsNullOrEmpty(reg.SettingName))
                    {
                        string s = kv.Key.Name;
                        if (2 < s.Length && s[1] == '_' && "gsm".Contains(s[0]))
                            s = s.Substring(2);
                        reg.SettingName = Utils.Capitalize(s);
                    }
                    reg.RegisterInto(this);
                    found = true;
                }
                FieldInfo f;
                if (!found && typeof(ISettingChangeHandler).IsAssignableFrom(t) 
                    && null != (f = t.GetField("CurrentValue", B | BindingFlags.FlattenHierarchy)))
                {
                    var reg = (RegBase)Activator.CreateInstance(typeof(Registration<>).MakeGenericType(f.FieldType));
                    reg.SettingName = t.Name;
                    reg.m_eventHandler = ((ISettingChangeHandler)Activator.CreateInstance(t)).OnSettingChange;
                    SetAccessors(reg, f);
                    reg.RegisterInto(this);
                }
            }
        }
        static List<WeakReference> g_asmDone;
    }

    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
    public class HasUpdatableGlobalSettingsAttribute : Attribute { }

    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, Inherited = false)]
    public class UpdatableSettingAttribute : Attribute
    {
        public string SettingName;
        public object DefaultValue;
        public int CacheTimeoutMs;
        /// <summary> true: use individual Timer to monitor this setting.
        /// false (default): use the shared timer of UpdatableSettings. </summary>
        public bool MonitorIndividually;
        /// <summary> The Type must be an ISettingChangeHandler implementation with default ctor.
        /// It will be instantiated once, at the time of registration. </summary>
        public Type OnUpdate;

        /// <summary> p_settingName, if omitted, defaults to the name of the field/property
        /// without g_/s_/m_ prefix and capitalized (e.g. g_foo => "Foo") </summary>
        public UpdatableSettingAttribute(string p_settingName) { SettingName = p_settingName; }
        public UpdatableSettingAttribute() { }
    }

    public interface ISettingChangeHandler
    {
        void OnSettingChange(UpdatableSettings.Slot p_slot, object p_newValue);
    }

    public abstract class UpdatableCfgSettingWithHandler<TDerived, T> : ISettingChangeHandler
    {
        protected UpdatableSettings.Slot<T> m_slot;

        public abstract void OnValueChanged(T p_newValue);

        void ISettingChangeHandler.OnSettingChange(UpdatableSettings.Slot p_slot, object p_newValue)
        {
            m_slot = (UpdatableSettings.Slot<T>)p_slot;
            OnValueChanged((T)p_newValue);
        }
    }
#endif
#endif
}