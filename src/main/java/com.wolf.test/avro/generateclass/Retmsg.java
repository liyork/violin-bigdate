/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.wolf.test.avro.generateclass;
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Retmsg extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Retmsg\",\"namespace\":\"com.wolf.test.hadoop.avro.generateclass\",\"fields\":[{\"name\":\"msg\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence msg;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Retmsg() {}

  /**
   * All-args constructor.
   */
  public Retmsg(java.lang.CharSequence msg) {
    this.msg = msg;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return msg;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: msg = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'msg' field.
   */
  public java.lang.CharSequence getMsg() {
    return msg;
  }

  /**
   * Sets the value of the 'msg' field.
   * @param value the value to set.
   */
  public void setMsg(java.lang.CharSequence value) {
    this.msg = value;
  }

  /** Creates a new Retmsg RecordBuilder */
  public static Retmsg.Builder newBuilder() {
    return new Retmsg.Builder();
  }
  
  /** Creates a new Retmsg RecordBuilder by copying an existing Builder */
  public static Retmsg.Builder newBuilder(Retmsg.Builder other) {
    return new Retmsg.Builder(other);
  }
  
  /** Creates a new Retmsg RecordBuilder by copying an existing Retmsg instance */
  public static Retmsg.Builder newBuilder(Retmsg other) {
    return new Retmsg.Builder(other);
  }
  
  /**
   * RecordBuilder for Retmsg instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Retmsg>
    implements org.apache.avro.data.RecordBuilder<Retmsg> {

    private java.lang.CharSequence msg;

    /** Creates a new Builder */
    private Builder() {
      super(Retmsg.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(Retmsg.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.msg)) {
        this.msg = data().deepCopy(fields()[0].schema(), other.msg);
        fieldSetFlags()[0] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Retmsg instance */
    private Builder(Retmsg other) {
            super(Retmsg.SCHEMA$);
      if (isValidValue(fields()[0], other.msg)) {
        this.msg = data().deepCopy(fields()[0].schema(), other.msg);
        fieldSetFlags()[0] = true;
      }
    }

    /** Gets the value of the 'msg' field */
    public java.lang.CharSequence getMsg() {
      return msg;
    }
    
    /** Sets the value of the 'msg' field */
    public Retmsg.Builder setMsg(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.msg = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'msg' field has been set */
    public boolean hasMsg() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'msg' field */
    public Retmsg.Builder clearMsg() {
      msg = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    public Retmsg build() {
      try {
        Retmsg record = new Retmsg();
        record.msg = fieldSetFlags()[0] ? this.msg : (java.lang.CharSequence) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
